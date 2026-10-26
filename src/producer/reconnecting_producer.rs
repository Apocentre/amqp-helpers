use std::{collections::BTreeMap, sync::{Arc, PoisonError, RwLock}, time::Duration};
use backon::{ExponentialBuilder, Retryable};
use eyre::{Result, eyre};
use lapin::{Error, types::{AMQPValue, ShortString}};
use log::{debug, info, warn};
use tokio::{sync::Mutex, time::timeout};
use super::retry_producer::{PublishRejected, RetryProducer};

const DEFAULT_PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

fn default_backoff() -> ExponentialBuilder {
  ExponentialBuilder::default()
    .with_jitter()
    .with_min_delay(Duration::from_millis(100))
    .with_max_delay(Duration::from_secs(5))
    .with_max_times(4)
    .with_total_delay(Some(Duration::from_secs(30)))
}

type ErrorHandler = Arc<dyn Fn(Error) + Send + Sync>;
type Headers = BTreeMap<ShortString, AMQPValue>;

struct Current {
  // bumped on every reconnect so concurrent failures of the same producer reconnect only once
  generation: u64,
  producer: Arc<RetryProducer>,
}

/// A `RetryProducer` that replaces its own connection when a publish fails because of it.
///
/// `publish` takes `&self` and never holds a lock while talking to the broker, so one instance can be shared
/// (e.g. in an `Arc`) between tasks without any external locking. When a publish fails with anything other than
/// a broker rejection ([`PublishRejected`]) or times out, the producer opens a new connection, re-declares the
/// topology, waits for an exponential backoff and publishes the message again, until the retries configured with
/// [`Self::with_backoff`] are used up. Concurrent failures share a single reconnect.
///
/// Delivery is therefore at-least-once: if the connection drops (or the publish times out) after the broker took
/// the message but before the confirm arrived, the retry publishes a duplicate.
pub struct ReconnectingProducer {
  config: Config,
  publish_timeout: Duration,
  backoff: ExponentialBuilder,
  current: RwLock<Current>,
  // serializes reconnects; never held while publishing
  reconnect_lock: Mutex<()>,
}

// everything needed to build a `RetryProducer` again
#[derive(Clone)]
struct Config {
  uri: String,
  exchange_name: String,
  queue_name: String,
  routing_key: String,
  ttl: u32,
  delay_ms: Option<u32>,
  on_connection_error: Option<ErrorHandler>,
  connect_timeout: Duration,
}

impl Config {
  async fn connect(&self) -> Result<RetryProducer> {
    let on_connection_error = self.on_connection_error.clone().map(|handler| move |err: Error| handler(err));

    timeout(
      self.connect_timeout,
      RetryProducer::new(
        &self.uri,
        &self.exchange_name,
        &self.queue_name,
        &self.routing_key,
        self.ttl,
        self.delay_ms,
        on_connection_error,
      ),
    )
    .await
    .map_err(|_| eyre!("timed out connecting to rabbitmq after {:?}", self.connect_timeout))?
  }
}

impl ReconnectingProducer {
  /// Connects and declares the topology, see [`RetryProducer::new`]. Returns an error instead of panicking when
  /// the broker is unreachable or rejects the topology.
  pub async fn new<E>(
    uri: &str,
    exchange_name: &str,
    queue_name: &str,
    routing_key: &str,
    ttl: u32,
    delay_ms: Option<u32>,
    on_connection_error: Option<E>,
  ) -> Result<Self>
  where
    E: Fn(Error) + Send + Sync + 'static
  {
    let config = Config {
      uri: uri.to_string(),
      exchange_name: exchange_name.to_string(),
      queue_name: queue_name.to_string(),
      routing_key: routing_key.to_string(),
      ttl,
      delay_ms,
      on_connection_error: on_connection_error.map(|handler| Arc::new(handler) as ErrorHandler),
      connect_timeout: DEFAULT_CONNECT_TIMEOUT,
    };
    let producer = Arc::new(config.connect().await?);

    Ok(Self {
      config,
      publish_timeout: DEFAULT_PUBLISH_TIMEOUT,
      backoff: default_backoff(),
      current: RwLock::new(Current {generation: 0, producer}),
      reconnect_lock: Mutex::new(()),
    })
  }

  /// How long a single publish attempt (including awaiting the confirm) may take before it counts as a connection
  /// failure. Defaults to 10 seconds.
  pub fn with_publish_timeout(mut self, publish_timeout: Duration) -> Self {
    self.publish_timeout = publish_timeout;

    self
  }

  /// How long connecting and declaring the topology may take when reconnecting. Defaults to 10 seconds.
  pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
    self.config.connect_timeout = connect_timeout;

    self
  }

  /// How failed publishes are retried: the delay between attempts, the number of retries (`with_max_times`) and the
  /// total time spent sleeping between them (`with_total_delay`). Defaults to up to 4 retries after the first
  /// attempt, with a jittered exponential delay from 100 milliseconds up to 5 seconds, and at most 30 seconds of
  /// sleeping in total. Use `with_max_times(0)` to disable retrying.
  ///
  /// The total delay only counts the sleeps, not the attempts themselves, so a call can run longer by up to one
  /// publish or connect timeout per attempt.
  pub fn with_backoff(mut self, backoff: ExponentialBuilder) -> Self {
    self.backoff = backoff;

    self
  }

  /// Same as [`RetryProducer::publish`], but when the connection is the problem it reconnects, backs off and
  /// publishes again, as configured by [`Self::with_backoff`]. A failed reconnect counts as a failed attempt.
  pub async fn publish(
    &self,
    routing_key: &str,
    payload: &[u8],
    persistent: bool,
    ttl: Option<u32>,
    headers: Option<Headers>,
  ) -> Result<()> {
    (|| self.publish_or_reconnect(routing_key, payload, persistent, ttl, headers.clone()))
      .retry(self.backoff)
      // the connection is fine, so a new one would be rejected the same way
      .when(|err| err.downcast_ref::<PublishRejected>().is_none())
      .notify(|err, delay| warn!(
        "Publishing to {} failed, retrying in {:?}: {:?}", self.config.exchange_name, delay, err,
      ))
      .await
  }

  // one attempt: publishes on the current connection and, if that fails because of the connection, replaces it
  // so that the next attempt (after the backoff) has a fresh one
  async fn publish_or_reconnect(
    &self,
    routing_key: &str,
    payload: &[u8],
    persistent: bool,
    ttl: Option<u32>,
    headers: Option<Headers>,
  ) -> Result<()> {
    let (generation, producer) = self.current();
    let err = match self.publish_once(&producer, routing_key, payload, persistent, ttl, headers).await {
      Ok(()) => return Ok(()),
      Err(err) if err.downcast_ref::<PublishRejected>().is_some() => return Err(err),
      Err(err) => err,
    };

    self.reconnect(generation).await
      .map_err(|reconnect_err| reconnect_err.wrap_err(format!("reconnecting after a failed publish ({err})")))?;

    Err(err)
  }

  async fn publish_once(
    &self,
    producer: &RetryProducer,
    routing_key: &str,
    payload: &[u8],
    persistent: bool,
    ttl: Option<u32>,
    headers: Option<Headers>,
  ) -> Result<()> {
    timeout(self.publish_timeout, producer.publish(routing_key, payload, persistent, ttl, headers))
      .await
      .map_err(|_| eyre!("publish timed out after {:?}", self.publish_timeout))?
  }

  async fn reconnect(&self, failed_generation: u64) -> Result<Arc<RetryProducer>> {
    let _guard = self.reconnect_lock.lock().await;
    let (generation, producer) = self.current();

    // another task already replaced the producer that failed
    if generation != failed_generation {
      return Ok(producer);
    }

    info!("Reconnecting producer of exchange {}", self.config.exchange_name);

    let new_producer = Arc::new(self.config.connect().await?);
    let old = std::mem::replace(
      &mut *self.current.write().unwrap_or_else(PoisonError::into_inner),
      Current {generation: generation + 1, producer: Arc::clone(&new_producer)},
    );
    let close_timeout = self.config.connect_timeout;

    // closing a dead connection can hang, so don't make the caller wait for it
    tokio::spawn(async move {
      match timeout(close_timeout, old.producer.close()).await {
        Ok(Err(err)) => debug!("Closing the replaced rabbitmq connection failed: {:?}", err),
        Err(_) => debug!("Closing the replaced rabbitmq connection timed out"),
        Ok(Ok(())) => {}
      }
    });

    Ok(new_producer)
  }

  fn current(&self) -> (u64, Arc<RetryProducer>) {
    let current = self.current.read().unwrap_or_else(PoisonError::into_inner);

    (current.generation, Arc::clone(&current.producer))
  }
}

#[cfg(test)]
mod tests {
  use std::time::{SystemTime, UNIX_EPOCH};
  use super::*;

  fn rabbitmq_uri() -> String {
    std::env::var("RABBITMQ_URI").unwrap_or_else(|_| "amqp://user:password@localhost:5672".to_string())
  }

  async fn new_producer(prefix: &str) -> (ReconnectingProducer, String) {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let name = format!("{prefix}_{nanos}");
    let routing_key = format!("{name}.new");
    let producer = ReconnectingProducer::new(
      &rabbitmq_uri(),
      &name,
      &name,
      &routing_key,
      1_000,
      Some(1_000),
      None::<fn(Error)>,
    )
    .await
    .unwrap();

    (producer, routing_key)
  }

  #[tokio::test]
  #[ignore = "requires a running RabbitMQ"]
  async fn publish_reconnects_after_the_connection_is_closed() {
    let (producer, routing_key) = new_producer("reconnect").await;

    producer.publish(&routing_key, b"before", true, None, None).await.unwrap();
    producer.current().1.close().await.unwrap();
    producer.publish(&routing_key, b"after", true, None, None).await.unwrap();

    assert_eq!(producer.current().0, 1);
    assert!(producer.current().1.is_connected());
  }

  #[tokio::test]
  #[ignore = "requires a running RabbitMQ"]
  async fn concurrent_failures_share_one_reconnect() {
    let (producer, routing_key) = new_producer("single_flight").await;
    let producer = Arc::new(producer);

    producer.current().1.close().await.unwrap();

    let publishes = (0..16).map(|_| {
      let producer = Arc::clone(&producer);
      let routing_key = routing_key.clone();

      tokio::spawn(async move { producer.publish(&routing_key, b"payload", true, None, None).await })
    });

    for publish in publishes.collect::<Vec<_>>() {
      publish.await.unwrap().unwrap();
    }

    assert_eq!(producer.current().0, 1);
  }

  #[tokio::test]
  #[ignore = "requires a running RabbitMQ"]
  async fn a_broker_rejection_does_not_reconnect() {
    let (producer, _) = new_producer("rejected").await;
    let err = producer.publish("some.other.key", b"payload", true, None, None).await.unwrap_err();

    assert!(matches!(err.downcast_ref::<PublishRejected>(), Some(PublishRejected::Unroutable {..})));
    assert_eq!(producer.current().0, 0);
  }

  #[tokio::test]
  #[ignore = "requires a running RabbitMQ"]
  async fn a_failed_reconnect_is_returned_as_an_error_without_panicking() {
    let (producer, routing_key) = new_producer("recover").await;
    // point the next reconnect at a port nothing listens on
    let broken = Config {uri: "amqp://user:password@127.0.0.1:1".to_string(), ..producer.config.clone()};
    let broken_producer = ReconnectingProducer {
      config: broken,
      publish_timeout: producer.publish_timeout,
      backoff: producer.backoff,
      current: RwLock::new(Current {generation: 0, producer: producer.current().1}),
      reconnect_lock: Mutex::new(()),
    };

    broken_producer.current().1.close().await.unwrap();

    assert!(broken_producer.publish(&routing_key, b"payload", true, None, None).await.is_err());
    assert_eq!(broken_producer.current().0, 0);
  }
}
