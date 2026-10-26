use std::future::Future;
use futures_lite::stream::StreamExt;
use eyre::{Result, WrapErr, bail};
use log::{debug, error, info, trace, warn};
use lapin::{
  Consumer, Error, message::Delivery, options::{
    BasicConsumeOptions, BasicQosOptions,
  }, types::FieldTable
};
use crate::core::{
  connection::Connection,
  types::MessageHandler,
};

pub struct RetryConsumer {
  pub consumer: Consumer,
}

impl RetryConsumer {
  pub async fn new<E>(
    uri: &str,
    queue_name: &str,
    consumer_tag: &str,
    prefetch_count: u16,
    on_connection_error: Option<E>,
  ) -> Result<Self>
  where
    E: FnMut(Error) + Send + 'static
  {
    let connection = Connection::new(uri, on_connection_error).await?;
    let channel = connection.create_channel().await?;

    debug!("Setting prefetch count to {}", prefetch_count);
    channel.basic_qos(prefetch_count, BasicQosOptions {global: false}).await?;

    // the consumer never declares the queue, so this fails if `RetryProducer::new` has not run yet
    let consumer = channel
    .basic_consume(
      queue_name.into(),
      consumer_tag.into(),
      BasicConsumeOptions::default(),
      FieldTable::default(),
    ).await.wrap_err_with(|| format!("cannot create consumer {consumer_tag} on queue {queue_name}"))?;

    info!("Consuming from queue {} with consumer tag {}", queue_name, consumer_tag);

    Ok(Self {consumer})
  }

  /// Calls `handler` with every delivery and its retry count, one at a time.
  ///
  /// This only ever returns an error: the consumer has no way to be stopped gracefully, so the stream ending
  /// (the consumer was cancelled, or the channel/connection closed), a delivery error, or an error returned by
  /// `handler` all stop the loop. The caller should treat that as a failure and restart the consumer.
  pub async fn consume<F>(&mut self, mut handler: MessageHandler<F>) -> Result<()>
  where
    F: Future<Output = Result<()>> + Send + 'static
  {
    while let Some(delivery) = self.consumer.next().await {
      let delivery = match delivery {
        Ok(delivery) => delivery,
        Err(error) => {
          error!("Consumer failed to receive a delivery, stopping: {:?}", error);

          return Err(error).wrap_err("consumer failed to receive a delivery");
        }
      };

      let retry_count = Self::get_retry_count(&delivery);
      trace!("Received delivery (delivery_tag: {}, retry_count: {})", delivery.delivery_tag, retry_count);

      handler(delivery, retry_count).await?;
    }

    error!("Consumer stream ended, the consumer was cancelled or the channel/connection was closed");

    bail!("consumer stream ended: the consumer was cancelled or the channel/connection was closed")
  }

  /// Never fails because of the header contents: any publisher can set an arbitrary `x-death` header, and a
  /// malformed one must not be able to panic or stop the consumer.
  pub fn get_retry_count(delivery: &Delivery) -> i64 {
    delivery.properties.headers().as_ref().map_or(0, Self::retry_count_from_headers)
  }

  fn retry_count_from_headers(headers: &FieldTable) -> i64 {
    // no x-death header means this is the first delivery
    let Some(x_death) = headers.inner().get("x-death") else {return 0};

    // As long as x-death exists the rest should be present. If it isn't (e.g. a publisher sent a bogus header)
    // we know the message went through at least one dead-letter hop, so fall back to 1 instead of failing.
    let count = x_death
    .as_array()
    .and_then(|entries| entries.as_slice().first())
    .and_then(|entry| entry.as_field_table())
    .and_then(|entry| entry.inner().get("count"))
    .and_then(|count| count.as_long_long_int());

    match count {
      Some(count) => count.max(0),
      None => {
        warn!("malformed x-death header, assuming retry count of 1: {:?}", x_death);
        1
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use lapin::types::{AMQPValue, FieldArray};
  use super::*;

  fn headers_with_x_death(x_death: AMQPValue) -> FieldTable {
    let mut headers = FieldTable::default();
    headers.insert("x-death".into(), x_death);
    headers
  }

  fn death_entry(count: AMQPValue) -> AMQPValue {
    let mut entry = FieldTable::default();
    entry.insert("count".into(), count);
    AMQPValue::FieldTable(entry)
  }

  #[test]
  fn no_x_death_is_zero() {
    assert_eq!(RetryConsumer::retry_count_from_headers(&FieldTable::default()), 0);
  }

  #[test]
  fn reads_count_of_the_first_entry() {
    let entries = FieldArray::from(vec![death_entry(AMQPValue::LongLongInt(3)), death_entry(AMQPValue::LongLongInt(9))]);
    let headers = headers_with_x_death(AMQPValue::FieldArray(entries));

    assert_eq!(RetryConsumer::retry_count_from_headers(&headers), 3);
  }

  #[test]
  fn empty_x_death_array_does_not_panic() {
    let headers = headers_with_x_death(AMQPValue::FieldArray(FieldArray::default()));

    assert_eq!(RetryConsumer::retry_count_from_headers(&headers), 1);
  }

  #[test]
  fn malformed_x_death_falls_back_to_one() {
    let wrong_type = headers_with_x_death(AMQPValue::Boolean(true));
    let wrong_entry = headers_with_x_death(AMQPValue::FieldArray(FieldArray::from(vec![AMQPValue::Boolean(true)])));
    let wrong_count = headers_with_x_death(AMQPValue::FieldArray(FieldArray::from(vec![death_entry(AMQPValue::Boolean(true))])));

    assert_eq!(RetryConsumer::retry_count_from_headers(&wrong_type), 1);
    assert_eq!(RetryConsumer::retry_count_from_headers(&wrong_entry), 1);
    assert_eq!(RetryConsumer::retry_count_from_headers(&wrong_count), 1);
  }

  #[test]
  fn negative_count_is_clamped() {
    let entries = FieldArray::from(vec![death_entry(AMQPValue::LongLongInt(-5))]);
    let headers = headers_with_x_death(AMQPValue::FieldArray(entries));

    assert_eq!(RetryConsumer::retry_count_from_headers(&headers), 0);
  }
}
