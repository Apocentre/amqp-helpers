use lapin::{
  Result,
  message::{Delivery},
  options::{BasicAckOptions},
};
use borsh::{BorshSerialize, BorshDeserialize};
use amqp::{
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

  retry_consumer.consume(Box::new(move |delivery: Result<Delivery>| async move {
    if let Ok(delivery) = delivery {
      let msg = Message::try_from_slice(&delivery.data).unwrap();
      println!("{:?}", msg);

      delivery
        .ack(BasicAckOptions::default())
        .await
        .expect("Failed to ack send_webhook_event message");
    }
  })).await;

}
