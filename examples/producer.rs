use tokio::time::{sleep, Duration};
use borsh::{BorshSerialize, BorshDeserialize};
use amqp::producer::retry_producer::RetryProducer;

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug)]
pub struct Message {
  pub name: String,
  pub age: u8,
}

#[tokio::main]
async fn main() {
  let uri = "amqp://user:password@localhost:5672";

  let producer = RetryProducer::new(
    uri,
    "example_exchange",
    "example_queue",
    "example.send",
    10_000, //  10 seconds
  ).await;

  for i in 0..100 {
    let msg = Message { name: format!("Name {}", i), age: i };
    producer.publish("example_exchange", "example.send", &msg.try_to_vec().unwrap()).await;
    
    sleep(Duration::from_secs(2)).await;
  }
}
