use lapin::Error;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};
use eyre::Result;
use amqp_helpers::producer::retry_producer::RetryProducer;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Message {
  pub name: String,
  pub age: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
  let uri = "amqp://user:password@localhost:5672";

  let producer = RetryProducer::new(
    uri,
    "example_exchange",
    "example_queue",
    "example.send",
    5_000, //  5 seconds
    None, Some(on_error)
  ).await?;

  for i in 0..1 {
    let msg = Message { name: format!("Name {}", i), age: i };
    producer.publish(
      "example_exchange",
      "example.send",
      &bitcode::serialize(&msg).unwrap(),
      true, None, None,
    ).await?;

    sleep(Duration::from_secs(2)).await;
  }

  Ok(())
}

fn on_error(error: Error) {
  println!("Error: {}", error);
}
