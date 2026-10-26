# amqp-helpers

Thin helpers on top of [lapin](https://github.com/amqp-rs/lapin) (RabbitMQ / AMQP 0-9-1) that give you a
producer and a consumer with a **built-in retry mechanism**. Retries are implemented purely with broker
features (dead-letter exchanges and TTLs), so there is no retry state in your application: a failed message
is parked in a *wait queue* for a while and is then automatically put back on the original queue.

- [Overview](#overview)
- [Quick start](#quick-start)
- [Topology](#topology)
- [How TTLs are used](#how-ttls-are-used)
- [How retries work](#how-retries-work)
- [Producer](#producer)
- [Consumers](#consumers)
- [Connection handling](#connection-handling)
- [Caveats](#caveats)

## Overview

| Module | Type | Purpose |
|---|---|---|
| `producer::retry_producer` | `RetryProducer` | Declares the whole exchange/queue topology and publishes messages |
| `producer::reconnecting_producer` | `ReconnectingProducer` | `RetryProducer` that reconnects itself when the connection breaks |
| `consumer::retry_consumer` | `RetryConsumer` | Push consumer (`basic.consume`) that hands each delivery plus its retry count to a closure |
| `consumer::consumer_runner` | `ConsumerRunner<M, H>` | Typed wrapper around `RetryConsumer`: deserializes the payload, calls your `Handler`, acks/nacks for you |
| `consumer::pull_consumer` | `PullConsumer` | Pull consumer (`basic.get`) that returns the next delivery plus its retry count |
| `core::connection` | `Connection` | Connection with auto-recovery and an error callback |
| `core::types` | `Handler`, `MessageHandler` | Handler trait / closure type used by the consumers |

Payloads are plain bytes. The examples and `ConsumerRunner` use [`bitcode`](https://crates.io/crates/bitcode)
(via serde) for (de)serialization.

## Quick start

Start a local RabbitMQ (see also `examples/README.md`):

```sh
docker run -d --hostname local-rabbit --name local-rabbit \
  -p 15672:15672 -p 5672:5672 \
  -e RABBITMQ_DEFAULT_USER=user -e RABBITMQ_DEFAULT_PASS=password \
  rabbitmq:3-management
```

Run the producer first (it declares the topology), then the consumer:

```sh
cargo run --example producer
cargo run --example consumer
```

The example consumer nacks every message, so you will see it come back every 5 seconds with an increasing
retry count. The management UI is at <http://localhost:15672> (`user` / `password`).

> **The consumer never declares anything.** It only calls `basic.consume` on an existing queue and returns an
> error (`cannot create consumer ...`) if the queue is missing. `RetryProducer::new` must have run at least once
> (from any process) before a consumer starts.

## Topology

`RetryProducer::new(uri, exchange_name, queue_name, routing_key, ttl, delay_ms, on_connection_error)` declares
the following. In the rest of this document `E` is `exchange_name` and `Q` is `queue_name`.

| Name | Kind | Declared arguments | Bindings |
|---|---|---|---|
| `E` | topic exchange | | |
| `E.dlx_retry_1` | topic exchange | | |
| `E.dlx_retry_2` | topic exchange | | |
| `Q` | queue | `x-dead-letter-exchange = E.dlx_retry_1` | `E` with `#`, `E.dlx_retry_2` with `#` |
| `Q.wait_retry` | queue | `x-dead-letter-exchange = E.dlx_retry_2`, `x-message-ttl = ttl` | `E.dlx_retry_1` with `#` |
| *optional, only when `delay_ms` is `Some`* | | | |
| `E.delay_ex` | direct exchange | | |
| `Q.entry_delay` | queue | `x-dead-letter-exchange = E`, `x-message-ttl = delay_ms` | `E.delay_ex` with `routing_key` |

All exchanges and queues are durable. No `x-dead-letter-routing-key` is set anywhere, so a dead-lettered
message keeps its original routing key. That is why every retry binding can simply use the catch-all `#`.

### Full topology

![Full topology of exchanges and queues](docs/diagrams/topology.png)

Arrows out of an exchange are bindings (labelled with their binding key); arrows into an exchange are
publishes or dead-lettering. The loop `Q → E.dlx_retry_1 → Q.wait_retry → E.dlx_retry_2 → Q` is the retry cycle.

> The diagrams in this document are pre-rendered PNGs with a solid background so they look the same in any
> viewer and in light or dark mode. Their Mermaid sources live in `docs/diagrams/*.mmd`; after editing one, run
> `docs/diagrams/render.sh` (needs Node and a local Chrome, see `puppeteer.config.json`) to regenerate the images.

### Why two retry exchanges?

A rejected message must not go straight back to `Q`, otherwise it would be redelivered immediately in a tight
loop. It needs a place to *wait* first, so the cycle is split in two hops:

1. `E.dlx_retry_1` is the DLX of `Q`. It receives rejected messages and routes them **only** to the wait
   queue.
2. `E.dlx_retry_2` is the DLX of `Q.wait_retry`. It receives messages whose wait is over and routes them
   **only** back to `Q`.

`E` itself is bound to `Q` too, but it is the entry point for new messages; it is never part of the retry
cycle (except when coming out of the optional delay queue).

## How TTLs are used

RabbitMQ supports two kinds of TTL, and this library uses both ([RabbitMQ TTL docs](https://www.rabbitmq.com/docs/ttl)).

| | Per-queue TTL (`x-message-ttl`) | Per-message TTL (`expiration` property) |
|---|---|---|
| Set where | Queue declaration argument | Message property at publish time |
| Used for here | `Q.wait_retry` (the `ttl` constructor arg) and `Q.entry_delay` (`delay_ms`) | `publish(..., ttl: Some(ms), ...)` |
| Applies to | Every message in that queue | Only that message |
| What happens on expiry | Message is dead-lettered to the queue's `x-dead-letter-exchange` | Same |

If both are present on a message, the **lower** value wins.

### Why per-queue TTL for the wait and delay queues

Messages only expire when they reach the **head** of a queue. With a per-queue TTL every message in the queue
has the same lifetime, so the head is always the one that expires first and nothing is stuck behind a
longer-lived message. With per-message TTLs of different lengths in the same queue, a message with a long TTL
at the head blocks shorter ones behind it (head-of-line blocking). Using a fixed per-queue TTL for the
wait and delay queues avoids that, and also means the retry interval is a property of the topology rather than
of each message.

### Per-message TTL in this library

`publish`'s `ttl` argument becomes the AMQP `expiration` property. It starts counting in the first queue the
message lands in:

- with `delay_ms` set, that is `Q.entry_delay`. If the per-message TTL is lower than `delay_ms`, the message
  leaves the delay queue sooner.
- otherwise it is `Q`. If the message is not consumed within its TTL it expires and is dead-lettered to
  `E.dlx_retry_1`, i.e. an expired message **enters the retry cycle** instead of being dropped.

RabbitMQ removes the `expiration` property when it dead-letters a message (the original value is kept in the
`x-death` header as `original-expiration`), so the per-message TTL does not apply again in the wait queue.
From then on, the wait queue's per-queue TTL is what governs the message.

### Summary of the three timers

![The three timers: delay_ms, ttl and the per-message TTL](docs/diagrams/timers.png)

## How retries work

A retry is simply a rejected message that is dead-lettered, waits in `Q.wait_retry` for `ttl` ms, and is then
dead-lettered back to `Q`.

1. The consumer receives the message from `Q`.
2. Processing fails, so the consumer **nacks without requeue**. `BasicNackOptions::default()` has
   `requeue: false`, which is what makes RabbitMQ dead-letter the message instead of putting it back.
3. `Q`'s DLX is `E.dlx_retry_1`, which routes the message (original routing key preserved) to
   `Q.wait_retry` via the `#` binding.
4. The message sits in `Q.wait_retry` until the queue's `x-message-ttl` (`ttl`) elapses. Nobody consumes
   from this queue; it exists only to let the TTL run.
5. On expiry the message is dead-lettered to `E.dlx_retry_2`, which routes it back to `Q` via the `#`
   binding.
6. The consumer receives it again, now with `retry_count >= 1`.

![Sequence of one retry cycle](docs/diagrams/retry-sequence.png)

### Counting retries

Each time RabbitMQ dead-letters a message it records it in the `x-death` header: one entry per
(queue, reason) pair with a `count`, and the entry for the most recent death is moved to the **front** of the
array. After a full cycle the front entry is `{ queue: "Q.wait_retry", reason: "expired", count: n }`, where
`n` is the number of times the message has gone around the loop.

`RetryConsumer::get_retry_count` reads `x-death[0].count` and returns it (or `0` when there is no `x-death`
header, i.e. the first delivery). A malformed `x-death` header (which any publisher can set) never fails or
panics the consumer: it is logged and counted as `1`. This number is what you receive as the `retry_count` argument in
`RetryConsumer::consume`, `Handler::handle`, and `PullConsumer::next`.

| Delivery | `x-death` header | `retry_count` |
|---|---|---|
| 1st (fresh) | absent | 0 |
| 2nd (after 1 nack) | `[{Q.wait_retry, expired, 1}, {Q, rejected, 1}]` | 1 |
| 3rd (after 2 nacks) | `[{Q.wait_retry, expired, 2}, {Q, rejected, 2}]` | 2 |

### Stopping retries

By default there is **no maximum**: a message that is nacked is retried forever. With `ConsumerRunner` you can
set one with `with_max_retries(n)`: once `handle` fails for a delivery whose `retry_count >= n`,
`Handler::on_retries_exhausted` is called (it logs by default) and, unless it returns an error, the message is
acked, which ends the cycle. Override the hook to park the message somewhere (e.g. a dead-letter queue) first.

With `RetryConsumer` or `PullConsumer` you look at `retry_count` yourself, for example:

```rust
if retry_count >= 5 {
    // give up: log it, publish it to a parking-lot queue, etc.
    return Ok(());          // returning Ok acks the message and ends the cycle
}
Err(eyre::eyre!("transient failure"))   // nack, goes around the cycle again
```

### Optional entry delay

When `delay_ms` is `Some`, `publish` sends to `E.delay_ex` instead of `E`. The message waits in
`Q.entry_delay` for `delay_ms` and is then dead-lettered to `E`, from where it follows the normal path into
`Q`. Use it to deliberately delay the first processing of every message.

![Optional entry delay path](docs/diagrams/entry-delay.png)

## Producer

```rust
let producer = RetryProducer::new(
    "amqp://user:password@localhost:5672",
    "example_exchange",   // E
    "example_queue",      // Q
    "example.send",       // routing key bound on E.delay_ex (only used when delay_ms is Some)
    5_000,                // ttl of Q.wait_retry: how long a nacked message waits before retry (ms)
    None,                 // delay_ms: optional entry delay (ms)
    Some(on_error),       // connection error callback
).await?;

producer.publish(
    "example.send",       // routing key. Published to E, or to E.delay_ex when delay is enabled
    &payload,             // bytes
    true,                 // persistent (delivery_mode = 2, survives a broker restart)
    None,                 // per-message TTL in ms (AMQP `expiration`)
    None,                 // extra headers
).await?;
```

`new` opens a connection and a channel, then declares everything described in [Topology](#topology).
Declaring is idempotent as long as the arguments match what already exists, so calling `new` from many
services is fine. Declarations wait for the broker's reply, so a conflict with an existing queue or exchange
(`PRECONDITION_FAILED`) is returned from `new` as an error. `publish` always targets the exchange given to `new`. `publish` sets `delivery_mode = 2` when `persistent` is true, adds custom headers, adds the
`expiration` property if a TTL is given, publishes with the `mandatory` flag, and awaits the publisher confirm.
`publish` returns an error if the broker nacks the message or returns it as unroutable. `new` enables
publisher confirms on the channel (`confirm_select`).

### `ReconnectingProducer`

`RetryProducer` is bound to the connection it was created with: once that connection is gone every publish
fails and you have to build a new producer yourself. `ReconnectingProducer` does that for you:

```rust
let producer = Arc::new(ReconnectingProducer::new(
    "amqp://user:password@localhost:5672",
    "example_exchange",
    "example_queue",
    "example.send",
    5_000,
    None,
    Some(on_error),       // Fn(lapin::Error) + Send + Sync, called on connection errors, including after reconnects
).await?
.with_publish_timeout(Duration::from_secs(5)));

producer.publish("example.send", &payload, true, None, None).await?;
```

`publish` takes the same arguments as `RetryProducer::publish` and `&self`, so a single `Arc<ReconnectingProducer>`
can be shared between tasks with no extra locking. If a publish fails with anything other than a broker
rejection (`PublishRejected`: nack or unroutable), or does not finish within the publish timeout (10 seconds by
default), the producer opens a new connection, re-declares the topology, closes the old connection in the
background, waits for an exponential backoff, and publishes the message again. Concurrent failures share a
single reconnect. A failed reconnect counts as a failed attempt. A broker rejection is returned without
retrying because a new connection would be rejected the same way.

Retrying is bounded by a [`backon`](https://docs.rs/backon) `ExponentialBuilder`. By default a publish is retried
up to 4 times, with a jittered delay that starts at 100 milliseconds and is capped at 5 seconds, and at most 30
seconds of sleeping in total. Change it with `with_backoff`; the last error is returned once a limit is hit.
`backon` is re-exported as `amqp_helpers::backon`, so you don't need to depend on it yourself:

```rust
use amqp_helpers::backon::ExponentialBuilder;

let producer = producer.with_backoff(
    ExponentialBuilder::default()
        .with_jitter()
        .with_min_delay(Duration::from_millis(200))
        .with_max_delay(Duration::from_secs(10))
        .with_max_times(6)                              // retries, not counting the first attempt
        .with_total_delay(Some(Duration::from_secs(60))), // sleeping only, attempts are not counted
);
```

Delivery is at-least-once: when the connection drops or the publish times out after the broker took the message
but before the confirm arrived, the retry publishes a duplicate.

## Consumers

### `RetryConsumer` (push, low level)

Opens a connection and channel, sets `basic.qos` with the given `prefetch_count` (per consumer, not global),
and starts a `basic.consume` with manual acknowledgements (`no_ack = false`).

`consume` takes a closure `FnMut(Delivery, i64) -> Future<Output = eyre::Result<()>>` called with each delivery
and its retry count. **The closure is responsible for acking or nacking.** Deliveries are processed one at a
time (the next one is not pulled from the stream until the closure's future completes); `prefetch_count` only
controls how many unacked messages the broker pushes ahead of time.

`consume` **only ever returns an error**: there is no graceful stop, so the stream ending (consumer cancelled,
channel or connection closed), a delivery error, or an `Err` returned by your closure all end the loop. Treat a
return as a failure and restart the consumer.

```rust
let mut consumer = RetryConsumer::new(uri, "example_queue", "basic_consumer", 1, Some(on_error)).await?;

let error = consumer.consume(Box::new(|delivery, retry_count| async move {
    // ... process ...
    delivery.nack(BasicNackOptions::default()).await?; // -> retry cycle
    Ok(())
})).await.unwrap_err();
```

### `ConsumerRunner<M, H>` (push, typed)

Implement `Handler<M>` for your type, where `M` is your (serde) message struct:

```rust
#[async_trait]
impl Handler<MyMessage> for MyHandler {
    async fn handle(&self, model: MyMessage, delivery: &Delivery, retry_count: i64) -> eyre::Result<()> { ... }
}

ConsumerRunner::new(uri, queue, consumer_tag, prefetch, MyHandler, Some(on_error)).await?
    .with_max_retries(5)    // optional, see "Stopping retries"
    .start().await?;
```

`start` has the same contract as `RetryConsumer::consume`: it only returns an error, including when an ack or
nack cannot be sent.

For every delivery the runner does the following:

![ConsumerRunner ack/nack decision flow](docs/diagrams/consumer-runner.png)

- **Undeserializable messages are not retried** (retrying could never succeed). `Handler::on_undeserializable`
  is called; by default it logs and returns `Ok(())`, so the message is acked and dropped. Override it to park
  the message first. Returning `Err(_)` nacks it instead.
- **`Ok(())` acks, `Err(_)` nacks** and therefore triggers a retry through the topology above. With
  `with_max_retries(n)`, a failure at `retry_count >= n` calls `Handler::on_retries_exhausted` instead (same
  contract as above).
- If lapin reports an ack/nack as not applied (the delivery belongs to a channel replaced by a connection
  recovery), a warning is logged and the broker redelivers the message, so handlers should tolerate duplicates.
- `ConsumerRunner` shares the handler through an `Arc`, so `handle` takes `&self`.

### `PullConsumer` (`basic.get`)

For cases where you want to control the polling loop yourself. `next()` calls `basic.get` with manual acks and
returns `Option<NextItem { delivery, retry_count }>` (`None` when the queue is empty). You ack/nack the
delivery yourself; a nack goes through the same retry cycle.

`recreate_channel()` replaces the channel. Closing a channel makes the broker return its unacked messages to
the ready state, so calling this periodically is a way to release deliveries that were fetched but never
acknowledged.

## Connection handling

`core::connection::Connection` wraps a lapin connection with:

- automatic recovery (`enable_auto_recover`) with a backoff capped at 3 attempts;
- a spawned task that listens for connection events for the lifetime of the connection. Every event is logged,
  and errors are also forwarded to your `on_connection_error` callback
  (`Option<FnMut(lapin::Error) + Send + 'static>`).

Connecting or creating a channel returns an error (without the URI, which usually contains credentials).
Use an `amqps://` URI to encrypt the connection (TLS via rustls is enabled by lapin's default features) and
load credentials from the environment or a secret store rather than hard-coding them; the `user:password`
URIs in the examples are for a local development broker only.

The library logs through the [`log`](https://crates.io/crates/log) facade; install a logger to see it.

## Caveats

- **Queue arguments are immutable.** Changing `ttl`, `delay_ms` or the DLX of an already-declared queue makes
  the broker reject the declare with `PRECONDITION_FAILED`, which `RetryProducer::new` returns as an error.
  Use a new queue name (or delete the old queue) when changing them.
- **Retry count with `delay_ms`.** A message coming out of `Q.entry_delay` already has an `x-death` entry
  (`{Q.entry_delay, expired, 1}`), so `get_retry_count` reports `1` on the very first delivery and the
  sequence becomes 1, 1, 2, 3, ... Treat `retry_count` as "number of dead-letter hops" in that mode.
- **Unlimited retries** unless you stop them, see [Stopping retries](#stopping-retries).
- **No payload size limit** is applied by this library; `ConsumerRunner` deserializes whatever the broker
  delivers. Configure the broker's `max_message_size` if producers are not trusted.
- **Exchanges are declared `auto_delete`.** RabbitMQ deletes an auto-delete exchange once its last binding is
  removed.
- **Unroutable publishes fail.** With `delay_ms` set, `E.delay_ex` is a direct exchange bound with the single
  `routing_key` given to `new`; publishing with any other routing key matches no binding. Because `publish` sets
  the `mandatory` flag, the broker returns the message and `publish` returns an error instead of dropping it.
  Without a delay, `E` is bound to `Q` with `#`, so every routing key reaches `Q`.
- **Publisher confirms are enabled** on the channel, so `publish` only returns `Ok` after the broker has taken
  ownership of the message (for a persistent message on a durable queue, after it is written to disk). A nack
  is returned as an error. The caller should retry; the message may or may not have been enqueued if the
  connection drops before the confirm arrives, so consumers should tolerate duplicates.
- Per-message TTL only takes effect when the message reaches the head of its queue, so an expired message
  stuck behind unexpired ones is not dropped or dead-lettered until it reaches the head.

## License

MIT, see `LICENSE`.
