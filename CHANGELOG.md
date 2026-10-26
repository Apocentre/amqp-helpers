# Changelog

## 4.1.0 (2026-10-26)

### Added

- `ReconnectingProducer`: a `RetryProducer` that replaces its own connection when a publish fails because of it
  (a lapin error or a timeout). `publish` takes `&self`, holds no lock while talking to the broker, shares one
  reconnect between concurrent failures and retries the publish once on the new connection. Failed connects are
  returned as errors, never panics. Broker rejections are returned as-is without reconnecting.
- `PublishRejected`: the typed error `RetryProducer::publish` returns when the broker nacks a message or returns
  it as unroutable. Its message text is unchanged; downcast the `eyre::Report` to tell it apart from connection
  errors.
- `RetryProducer::is_connected` and `RetryProducer::close`.

### Changed

- `tokio` now needs the `sync` and `time` features.

## 4.0.0 (2026-10-26)

### Breaking changes

- `Connection::new`, `Connection::create_channel`, `PullConsumer::recreate_channel` and the consumer
  constructors no longer panic. Failures are returned as `eyre::Result` errors. `Connection::new` and
  `Connection::create_channel` now return `Result`. The connection URI is left out of error messages because it
  usually contains credentials.
- `RetryProducer::publish` no longer takes the exchange name as its first argument. It publishes to the exchange
  given to `RetryProducer::new` (or to its delay exchange). Publishing with a different exchange name used to
  target an undeclared delay exchange and close the channel.
- `MessageHandler`, the closure given to `RetryConsumer::consume`, is now
  `FnMut(Delivery, i64) -> Future<Output = eyre::Result<()>>`. It no longer receives `lapin::Result<Delivery>`,
  and an `Err` stops the consumer.
- `RetryConsumer::consume` and `ConsumerRunner::start` now return an error when the consumer stream ends
  (consumer cancelled, channel or connection closed) or yields a delivery error. They used to return `Ok(())`
  and silently ignore delivery errors. They only ever return errors now, so callers should restart the consumer.
- `ConsumerRunner` returns ack/nack failures instead of panicking.
- `RetryConsumer::get_retry_count` takes a `&Delivery` and returns `i64`.
- Exchange and queue declarations and bindings wait for the broker's reply (`nowait: false`), so a conflict with
  an existing queue or exchange (`PRECONDITION_FAILED`) is returned from `RetryProducer::new` instead of
  closing the channel asynchronously.

### Added

- `ConsumerRunner::with_max_retries` stops the retry cycle once a failing message reaches the given
  `retry_count`.
- `Handler::on_undeserializable` and `Handler::on_retries_exhausted` hooks, so messages can be parked (e.g. in a
  dead-letter queue) before they are dropped. Both have default implementations that log and return `Ok(())`,
  so existing handlers compile unchanged and keep dropping undeserializable messages.
- Logging through the `log` facade for connection events, consuming, publishing and topology declaration.
- Integration tests for the consumer (malformed `x-death` header, max retries).

### Fixed

- A malformed `x-death` header (an empty array, or entries of the wrong type) could panic the consumer or stop
  its loop and leave the message unacked, so one bad message could crash-loop a queue. It is now logged and
  counted as a retry count of `1`. `PullConsumer::next` no longer leaks the unacked delivery in this case.
- The connection error listener stopped at the first non-error event (every recovery emits `Connected`), after
  which `on_connection_error` was never called again.
- `ConsumerRunner` now logs a warning when lapin reports an ack/nack as not applied, which happens for
  deliveries of a channel replaced by a connection recovery. The broker redelivers those messages.

### Changed

- Requires `lapin` 4.12.3 or newer.
- Removed the unused `tokio-executor-trait` and `tokio-reactor-trait` dependencies.
- `println!` in `ConsumerRunner::start` replaced with `log::info!`.
