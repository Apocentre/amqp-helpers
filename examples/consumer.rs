use lapin::{
  Result,
  message::{Delivery},
  options::{BasicAckOptions, BasicNackOptions},
};
use borsh::{BorshSerialize, BorshDeserialize};
use amqp_helpers::{
  consumer::retry_consumer::RetryConsumer,
};

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug)]
pub struct Message {
  pub name: String,
  pub age: u8,
}

#[tokio::main]
async fn main() {
  let uri = "amqp://user:password@localhost:5672";

  let mut retry_consumer = RetryConsumer::new(
    uri,
    "example_queue",
    "basic_consumer",
  ).await;

  retry_consumer.consume(Box::new(move |delivery: Result<Delivery>, retry_count: i64| async move {
    if let Ok(delivery) = delivery {
      println!("Retry count {:?}", retry_count);
      let msg = Message::try_from_slice(&delivery.data).unwrap();
      println!("{:?}", msg);

      delivery
        .nack(BasicNackOptions::default())
        .await
        .expect("Failed to ack send_webhook_event message");
    }
  })).await;

}
