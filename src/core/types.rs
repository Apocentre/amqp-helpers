use async_trait::async_trait;
use log::error;
use serde::de::DeserializeOwned;
use lapin::message::Delivery;

/// Called by `RetryConsumer::consume` with every delivery and its retry count. The returned future is
/// responsible for acking/nacking the delivery. An `Err` stops the consumer and is returned from `consume`.
pub type MessageHandler<F> = Box<dyn FnMut(Delivery, i64) -> F + Send>;

#[async_trait]
pub trait Handler<M: DeserializeOwned + Send + Sync> {
  /// Process a message. `Ok(())` acks it, `Err(_)` nacks it, which sends it through the retry cycle.
  async fn handle(&self, model: M, delivery: &Delivery, retry_count: i64) -> eyre::Result<()>;

  /// Called when the payload cannot be deserialized into `M`. Retrying can never fix such a message.
  ///
  /// Returning `Ok(())` (the default, which logs the failure) acks, i.e. drops, the message. Override this to
  /// park the message somewhere (e.g. publish it to a dead-letter queue) before it is dropped. Returning
  /// `Err(_)` nacks it instead, which sends it through the retry cycle.
  async fn on_undeserializable(
    &self,
    delivery: &Delivery,
    error: &bitcode::Error,
    retry_count: i64,
  ) -> eyre::Result<()> {
    error!(
      "Dropping undeserializable message (delivery_tag: {}, routing_key: {}, retry_count: {}, payload_bytes: {}): {:?}",
      delivery.delivery_tag,
      delivery.routing_key,
      retry_count,
      delivery.data.len(),
      error,
    );

    Ok(())
  }

  /// Called when `handle` failed and `retry_count` has reached the limit set with
  /// `ConsumerRunner::with_max_retries`.
  ///
  /// Returning `Ok(())` (the default, which logs the failure) acks, i.e. drops, the message and ends the retry
  /// cycle. Override this to park the message somewhere first. Returning `Err(_)` nacks it, so the cycle
  /// continues.
  async fn on_retries_exhausted(
    &self,
    delivery: &Delivery,
    retry_count: i64,
    error: &eyre::Report,
  ) -> eyre::Result<()> {
    error!(
      "Dropping message after exhausting retries (delivery_tag: {}, routing_key: {}, retry_count: {}): {:?}",
      delivery.delivery_tag,
      delivery.routing_key,
      retry_count,
      error,
    );

    Ok(())
  }
}
