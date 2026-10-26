use std::{
  collections::BTreeMap,
  sync::{atomic::{AtomicI64, AtomicUsize, Ordering}, Arc},
  time::{Duration, SystemTime, UNIX_EPOCH},
};
use async_trait::async_trait;
use eyre::eyre;
use serde::{Deserialize, Serialize};
use tokio::{sync::Notify, time::timeout};
use amqp_helpers::{
  consumer::{consumer_runner::ConsumerRunner, retry_consumer::RetryConsumer},
  core::types::Handler,
  lapin::{
    message::Delivery, options::BasicAckOptions, types::{AMQPValue, FieldArray, ShortString}, Error,
  },
  producer::retry_producer::RetryProducer,
};

const WAIT: Duration = Duration::from_secs(15);

fn rabbitmq_uri() -> String {
  std::env::var("RABBITMQ_URI").unwrap_or_else(|_| "amqp://user:password@localhost:5672".to_string())
}

fn unique_name(prefix: &str) -> String {
  let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();

  format!("{prefix}_{nanos}")
}

async fn new_producer(name: &str, routing_key: &str, ttl: u32) -> RetryProducer {
  RetryProducer::new(&rabbitmq_uri(), name, name, routing_key, ttl, None, None::<fn(Error)>).await.unwrap()
}

#[derive(Serialize, Deserialize)]
struct Message {
  name: String,
}

#[tokio::test]
#[ignore = "requires a running RabbitMQ"]
async fn malformed_x_death_header_does_not_stop_the_consumer() {
  let name = unique_name("malformed_x_death");
  let producer = new_producer(&name, "any", 1_000).await;

  // any publisher can set this header; an empty array used to panic the consumer
  let headers = BTreeMap::from([
    (ShortString::from("x-death"), AMQPValue::FieldArray(FieldArray::default())),
  ]);
  producer.publish("any", b"payload", true, None, Some(headers)).await.unwrap();

  let mut consumer = RetryConsumer::new(&rabbitmq_uri(), &name, &unique_name("tag"), 1, None::<fn(Error)>).await.unwrap();
  let retry_count = Arc::new(AtomicI64::new(-1));
  let seen = Arc::clone(&retry_count);

  // `consume` only returns on error, so fail on purpose once the delivery was seen
  let result = timeout(WAIT, consumer.consume(Box::new(move |delivery: Delivery, count: i64| {
    let seen = Arc::clone(&seen);

    async move {
      seen.store(count, Ordering::SeqCst);
      delivery.ack(BasicAckOptions::default()).await?;

      Err(eyre!("done"))
    }
  }))).await.expect("consumer did not receive the message");

  assert_eq!(result.unwrap_err().to_string(), "done");
  assert_eq!(retry_count.load(Ordering::SeqCst), 1);
}

#[derive(Default)]
struct AlwaysFails {
  attempts: AtomicUsize,
  exhausted: Notify,
  exhausted_at: AtomicI64,
}

#[async_trait]
impl Handler<Message> for Arc<AlwaysFails> {
  async fn handle(&self, _model: Message, _delivery: &Delivery, _retry_count: i64) -> eyre::Result<()> {
    self.attempts.fetch_add(1, Ordering::SeqCst);

    Err(eyre!("always fails"))
  }

  async fn on_retries_exhausted(&self, _delivery: &Delivery, retry_count: i64, _error: &eyre::Report) -> eyre::Result<()> {
    self.exhausted_at.store(retry_count, Ordering::SeqCst);
    self.exhausted.notify_one();

    Ok(())
  }
}

#[tokio::test]
#[ignore = "requires a running RabbitMQ"]
async fn max_retries_stops_the_retry_cycle() {
  let name = unique_name("max_retries");
  let producer = new_producer(&name, "any", 200).await;
  producer.publish("any", &bitcode::serialize(&Message {name: "x".into()}).unwrap(), true, None, None).await.unwrap();

  let handler = Arc::new(AlwaysFails::default());
  let runner = ConsumerRunner::new(
    rabbitmq_uri(), name.clone(), unique_name("tag"), 1, Arc::clone(&handler), None::<fn(Error)>,
  ).await.unwrap().with_max_retries(2);

  let task = tokio::spawn(runner.start());
  timeout(WAIT, handler.exhausted.notified()).await.expect("retries were never exhausted");

  // original attempt + 2 retries, then the message is acked and the cycle ends
  assert_eq!(handler.attempts.load(Ordering::SeqCst), 3);
  assert_eq!(handler.exhausted_at.load(Ordering::SeqCst), 2);

  tokio::time::sleep(Duration::from_millis(800)).await;
  assert_eq!(handler.attempts.load(Ordering::SeqCst), 3, "message was retried after exhausting its retries");

  task.abort();
}
