use std::{
  marker::PhantomData, time::Instant, sync::Arc,
};
use eyre::{Result, WrapErr};
use log::{debug, error, info, trace, warn};
use serde::de::DeserializeOwned;
use lapin::{
  message::Delivery, options::{BasicAckOptions, BasicNackOptions}, Error
};
use crate::core::types::Handler;
use super::retry_consumer::RetryConsumer;

pub struct ConsumerRunner<M, H>
where
  M: DeserializeOwned + Send + Sync,
  H: Handler<M> + Send + Sync + 'static
{
  retry_consumer: RetryConsumer,
  handler: Arc<H>,
  max_retries: Option<i64>,
  phantom: PhantomData<M>,
}

impl <M, H> ConsumerRunner<M, H>
where
  M: DeserializeOwned  + Send + Sync + 'static,
  H: Handler<M> + Send + Sync + 'static
{
  pub async fn new<E>(
    rabbitmq_uri: String,
    queue_name: String,
    consumer_tag: String,
    prefetch_count: u16,
    handler: H,
    on_connection_error: Option<E>,
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
    ).await?;

    Ok(Self {
      retry_consumer,
      handler: Arc::new(handler),
      max_retries: None,
      phantom: PhantomData,
    })
  }

  /// Stop retrying a message once `handle` has failed for it with `retry_count >= max_retries`.
  /// `Handler::on_retries_exhausted` is then called and, unless it fails, the message is acked (dropped).
  /// Without this a message that keeps failing is retried forever.
  ///
  /// Note that `retry_count` counts dead-letter hops, so it starts at 1 when `delay_ms` is used.
  pub fn with_max_retries(mut self, max_retries: i64) -> Self {
    self.max_retries = Some(max_retries);
    self
  }

  /// Runs until the consumer stops, which is always reported as an error: the consumer was cancelled, the
  /// channel/connection was closed, or an ack/nack could not be sent. The caller should restart it.
  pub async fn start(mut self) -> Result<()> {
    info!("Running... (max_retries: {:?})", self.max_retries);

    let max_retries = self.max_retries;
    let handler = self.handler;

    self.retry_consumer.consume(Box::new(move |delivery: Delivery, retry_count: i64| {
      let handler = Arc::clone(&handler);

      async move {
        let start = Instant::now();
        let result = process::<M, H>(&handler, &delivery, retry_count, max_retries).await;

        trace!("Msg exec time: {:?}", start.elapsed());

        result
      }
    })).await
  }
}

/// Handles one delivery and acks/nacks it. Only fails when the ack/nack itself cannot be sent.
async fn process<M, H>(
  handler: &H,
  delivery: &Delivery,
  retry_count: i64,
  max_retries: Option<i64>,
) -> Result<()>
where
  M: DeserializeOwned + Send + Sync,
  H: Handler<M> + Send + Sync + 'static
{
  let event = match bitcode::deserialize::<M>(&delivery.data) {
    Ok(event) => event,
    Err(error) => {
      return match handler.on_undeserializable(delivery, &error, retry_count).await {
        // ack so the message doesn't stay unacked on the channel until the channel closes.
        Ok(()) => ack(delivery).await,
        Err(hook_error) => {
          error!("on_undeserializable failed (delivery_tag: {}): {:?}", delivery.delivery_tag, hook_error);

          nack(delivery).await
        }
      }
    }
  };

  match handler.handle(event, delivery, retry_count).await {
    Ok(()) => {
      trace!("Msg acked");

      ack(delivery).await
    },
    Err(error) => {
      if max_retries.is_some_and(|max_retries| retry_count >= max_retries) {
        warn!("Msg exhausted its retries (delivery_tag: {}, retry_count: {})", delivery.delivery_tag, retry_count);

        match handler.on_retries_exhausted(delivery, retry_count, &error).await {
          Ok(()) => return ack(delivery).await,
          Err(hook_error) => {
            error!("on_retries_exhausted failed (delivery_tag: {}): {:?}", delivery.delivery_tag, hook_error);
          }
        }
      }

      error!("Msg nacked {:?}", error);

      nack(delivery).await
    }
  }
}

async fn ack(delivery: &Delivery) -> Result<()> {
  let applied = delivery
  .ack(BasicAckOptions::default())
  .await
  .wrap_err_with(|| format!("failed to ack message (delivery_tag: {})", delivery.delivery_tag))?;

  warn_if_not_applied(applied, "ack", delivery);

  Ok(())
}

async fn nack(delivery: &Delivery) -> Result<()> {
  let applied = delivery
  .nack(BasicNackOptions::default())
  .await
  .wrap_err_with(|| format!("failed to nack message (delivery_tag: {})", delivery.delivery_tag))?;

  warn_if_not_applied(applied, "nack", delivery);

  Ok(())
}

/// lapin reports `Ok(false)` when the ack/nack was not sent, e.g. the delivery belongs to a channel that was
/// replaced by a connection recovery. The broker will redeliver the message, so the handler may see it twice.
fn warn_if_not_applied(applied: bool, action: &str, delivery: &Delivery) {
  if applied {
    debug!("{} applied (delivery_tag: {})", action, delivery.delivery_tag);
  } else {
    warn!(
      "{} not applied (delivery_tag: {}); the message will be redelivered by the broker",
      action,
      delivery.delivery_tag,
    );
  }
}
