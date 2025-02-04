use std::{
  marker::PhantomData, time::Instant, sync::Arc,
};
use eyre::Result;
use log::trace;
use borsh::BorshDeserialize;
use lapin::{
  message::Delivery, options::{BasicAckOptions, BasicNackOptions}, Error, Result as LapinResult
};
use crate::core::types::Handler;
use super::retry_consumer::RetryConsumer;

pub struct ConsumerRunner<M, H>
where
  M: BorshDeserialize + Send + Sync,
  H: Handler<M> + Send + Sync + 'static
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
  pub async fn new<E>(
    rabbitmq_uri: String,
    queue_name: String,
    consumer_tag: String,
    prefetch_count: u16,
    handler: H,
    on_connection_error: Option<E>,
    on_channel_error: Option<E>,
  ) -> Result<Self>
  where
    E: FnMut(Error) + Send + 'static
  {
    let retry_consumer = RetryConsumer::new(
      &rabbitmq_uri,
      &queue_name,
      &consumer_tag,
      prefetch_count,
      on_connection_error,
      on_channel_error,
    ).await?;

    Ok(Self {
      retry_consumer,
      handler: Arc::new(handler),
      phantom: PhantomData::default(),
    })
  }

  pub async fn start(mut self) -> Result<()> {
    println!("Running...");

    self.retry_consumer.consume(Box::new(move |delivery: LapinResult<Delivery>, retry_count: i64| {
      let handler = Arc::clone(&self.handler);

      async move {
        // let mut handler = handler.lock().await;

        if let Ok(delivery) = delivery {
          let start = Instant::now();
          let event = M::try_from_slice(&delivery.data).unwrap();

          let result = handler.handle(
            event, 
            &delivery,
            retry_count,
          ).await;

          match result {
            Ok(()) => {
              trace!("Msg acked");

              delivery
              .ack(BasicAckOptions::default())
              .await
              .expect("ack");
            },
            Err(error) => { 
              trace!("Msg nacked {:?}", error);

              delivery
              .nack(BasicNackOptions::default())
              .await
              .expect("nack");
            }
          }

          let duration = start.elapsed();
          trace!("Msg exec time: {:?}", duration);
        }
    }})).await?;

    Ok(())
  }
}
