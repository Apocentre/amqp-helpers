use lapin::{
  Error, message::Delivery, options::BasicNackOptions,
};
use amqp_helpers::{
  consumer::retry_consumer::RetryConsumer,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Debug)]
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
    1,
    Some(on_error)
  ).await.unwrap();

  // `consume` only returns when the consumer stopped (cancelled, connection closed, or the handler failed)
  let error = retry_consumer.consume(Box::new(move |delivery: Delivery, retry_count: i64| async move {
    println!("Retry count {:?}", retry_count);
    let msg = bitcode::deserialize::<Message>(&delivery.data)?;
    println!("{:?}", msg);

    delivery.nack(BasicNackOptions::default()).await?;

    Ok(())
  })).await.unwrap_err();

  println!("Consumer stopped: {:?}", error);
}

fn on_error(error: Error) {
  println!("Error: {}", error);
}
