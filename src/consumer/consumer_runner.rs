use std::{
  sync::Arc,
  marker::PhantomData,
};
use eyre::Result;
use borsh::BorshDeserialize;
use lapin::{
  Result as LapinResult,
  message::{Delivery},
  options::{BasicAckOptions, BasicNackOptions},
};
use crate::core::types::Handler;
use super::retry_consumer::RetryConsumer;

pub struct ConsumerRunner<M, H>
where
  M: BorshDeserialize + Send + Sync,
  H: Handler<M> + Send  + Sync + 'static
{
  retry_consumer: RetryConsumer,
  handler: Arc<H>,
  phantom: PhantomData<M>,
}

impl <M, H> ConsumerRunner<M, H>
where
  M: BorshDeserialize  + Send + Sync,
  H: Handler<M> + Send + Sync + 'static
{
  pub async fn new(
    rabbitmq_uri: String,
    queue_name: String,
    consumer_tag: String,
    handler: Arc<H>,
  ) -> Self {
    let retry_consumer = RetryConsumer::new(
      &rabbitmq_uri,
      &queue_name,
      &consumer_tag,
    ).await;

    Self {
      retry_consumer,
      handler,
      phantom: PhantomData::default(),
    }
  }

  pub async fn start(&mut self) -> Result<()> {
    println!("Running...");

    let handler = Arc::clone(&self.handler);

    self.retry_consumer.consume(Box::new(move |delivery: LapinResult<Delivery>, _| {
      let handler = Arc::clone(&handler);

      async move {  
        if let Ok(delivery) = delivery {
          let event = M::try_from_slice(&delivery.data).unwrap();

          let result = handler.handle(
            event, 
            &delivery,
          ).await;

          match result {
            Ok(()) => {
              delivery
              .ack(BasicAckOptions::default())
              .await
              .expect("ack");
            },
            Err(_) => {
              delivery
              .nack(BasicNackOptions::default())
              .await
              .expect("nack");
            }
          }
        }
    }})).await?;

    Ok(())
  }
}
