use std::time::{SystemTime, UNIX_EPOCH};
use amqp_helpers::{lapin::Error, producer::retry_producer::RetryProducer};

fn rabbitmq_uri() -> String {
  std::env::var("RABBITMQ_URI").unwrap_or_else(|_| "amqp://user:password@localhost:5672".to_string())
}

fn unique_name(prefix: &str) -> String {
  let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();

  format!("{prefix}_{nanos}")
}

async fn new_producer(name: &str, routing_key: &str, delay_ms: Option<u32>) -> RetryProducer {
  RetryProducer::new(
    &rabbitmq_uri(),
    name,
    name,
    routing_key,
    1_000,
    delay_ms,
    None::<fn(Error)>,
  )
  .await
  .unwrap()
}

#[tokio::test]
#[ignore = "requires a running RabbitMQ"]
async fn publish_to_a_bound_routing_key_is_confirmed() {
  let name = unique_name("confirmed");
  let routing_key = format!("{name}.new");
  let producer = new_producer(&name, &routing_key, Some(1_000)).await;

  producer.publish(&name, &routing_key, b"payload", true, Some(500), None).await.unwrap();
}

#[tokio::test]
#[ignore = "requires a running RabbitMQ"]
async fn publish_to_an_unbound_routing_key_fails_instead_of_being_dropped() {
  let name = unique_name("unroutable");
  let routing_key = format!("{name}.new");
  let producer = new_producer(&name, &routing_key, Some(1_000)).await;

  let result = producer.publish(&name, "some.other.key", b"payload", true, None, None).await;

  assert!(result.unwrap_err().to_string().contains("unroutable"));
}
