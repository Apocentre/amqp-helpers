use std::{
	future::Future
};
use futures_lite::stream::StreamExt;
use eyre::{Result, ContextCompat};
use lapin::{
  Result as LapinResult,
  Consumer,
  options::{
    BasicConsumeOptions,
  },
  message::{Delivery},
  types::{FieldTable},
};
use crate::{
  core::{
    connection::Connection,
    types::{MessageHandler},
  },
};

pub struct RetryConsumer {
  pub consumer: Consumer,
}

impl RetryConsumer {
  pub async fn new(
    uri: &str,
    queue_name: &str,
    consumer_tag: &str,
  ) -> Self {
    let connection = Connection::new(uri).await;
    let channel = connection.create_channel().await;    
    let consumer = channel.basic_consume(
      queue_name,
      consumer_tag,
      BasicConsumeOptions::default(),
      FieldTable::default(),
    ).await.expect("cannot create consumer");

    Self {consumer}
  }

  pub async fn consume<F: Future<Output = ()> + Send + 'static>(&mut self, handler: MessageHandler<F>) -> Result<()> {
    while let Some(delivery) = self.consumer.next().await {
      let retry_count = Self::get_retry_count(&delivery)?;
      handler(delivery, retry_count).await;
    }

    Ok(())
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
