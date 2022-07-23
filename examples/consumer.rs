use lapin::{
  message::{DeliveryResult},
  options::{BasicAckOptions},
};
use borsh::{BorshSerialize, BorshDeserialize};
use amqp::{
  core::types::DeliveryHandler,
  consumer::retry_consumer::RetryConsumer,
};

#[derive(BorshSerialize, BorshDeserialize, PartialEq, Debug)]
pub struct Message {
  pub name: String,
  pub age: u8,
}

#[tokio::main]
async fn main() {
  let uri = "amqp://localhost:5672";

  let consumer = RetryConsumer::new(
    uri,
    "example_queue",
    "basic_consumer",
  ).await;

  let handler = DeliveryHandler(Box::new(move |delivery: DeliveryResult| async move {
    let delivery = match delivery {
      // Carries the delivery alongside its channel
      Ok(Some(delivery)) => delivery,
      // The consumer got canceled
      Ok(None) => return,
      // Carries the error and is always followed by Ok(None)
      Err(error) => {
          dbg!("Failed to consume queue message {}", error);
          return;
      }
    };

    // Do something with the delivery data (The message payload)
    println!("{:?}", delivery);

    let msg = Message::try_from_slice(&delivery.data).unwrap();
    println!("{:?}", msg);

    delivery
      .ack(BasicAckOptions::default())
      .await
      .expect("Failed to ack send_webhook_event message");
  }));

  consumer.consume(handler);
}

