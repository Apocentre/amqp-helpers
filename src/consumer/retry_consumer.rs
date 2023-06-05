use std::{
	future::Future
};
use futures_lite::stream::StreamExt;
use eyre::{Result, ContextCompat};
use lapin::{
  Result as LapinResult,
  Consumer,
  options::{
    BasicConsumeOptions, BasicQosOptions, BasicGetOptions,
  },
  message::{Delivery, BasicGetMessage},
  types::{FieldTable}, Channel,
};
use crate::{
  core::{
    connection::Connection,
    types::{MessageHandler},
  },
};

pub struct NextItem {
  pub delivery: Delivery,
  pub retry_count: i64,
}

pub struct RetryConsumer {
  channel: Channel,
  consumer: Consumer,
  queue_name: String,
}

impl RetryConsumer {
  pub async fn new(
    uri: &str,
    queue_name: &str,
    consumer_tag: &str,
    prefetch_count: u16,
  ) -> Result<Self> {
    let connection = Connection::new(uri).await;
    let channel = connection.create_channel().await;
    channel.basic_qos(prefetch_count, BasicQosOptions {global: false}).await?;

    let consumer = channel
    .basic_consume(
      queue_name,
      consumer_tag,
      BasicConsumeOptions::default(),
      FieldTable::default(),
    ).await.expect("cannot create consumer");

    Ok(Self {
      channel,
      consumer,
      queue_name: queue_name.to_string(),
    })
  }

  pub async fn consume<F>(&mut self, mut handler: MessageHandler<F>) -> Result<()>
  where
    F: Future<Output = ()> + Send + 'static
  {
    while let Some(delivery) = self.consumer.next().await {
      let retry_count = Self::get_retry_count(&delivery)?;
      handler(delivery, retry_count).await;
    }

    Ok(())
  }

  pub async fn next(&mut self) -> Result<Option<NextItem>> {
    let basic_get_result = self.channel.basic_get(&self.queue_name, BasicGetOptions {no_ack: true}).await?;
    let Some(BasicGetMessage {delivery, ..}) = basic_get_result else {return Ok(None)};
    let delivery = Ok(delivery);
    let retry_count = Self::get_retry_count(&delivery)?;

    let next_item = NextItem {
      delivery: delivery?,
      retry_count
    };
    
    Ok(Some(next_item))
  }

  fn get_retry_count(delivery: &LapinResult<Delivery>) -> Result<i64> {
    if let Ok(delivery) = delivery {
      let headers = delivery.properties.headers();

      if let Some(headers) = headers {
        let x_death = headers.inner().get("x-death");

        // As long as x_death exist the rest should be present. But we still need to monitor this
        if let Some(x_death) = x_death {
          return Ok(
            x_death
            .as_array()
            .context("")?
            .as_slice()[0]
            .as_field_table()
            .context("")?
            .inner()
            .get("count")
            .context("")?
            .as_long_long_int()
            .unwrap_or(1)
          )
        }
      }
    }

    Ok(1)
  }
}
