use std::{
	future::Future
};
use futures_lite::stream::StreamExt;
use lapin::{
  Result,
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

  pub async fn consume<F: Future<Output = ()> + Send + 'static>(&mut self, handler: MessageHandler<F>) {
    while let Some(delivery) = self.consumer.next().await {
      let retry_count = Self::get_retry_count(&delivery);
      handler(delivery, retry_count).await;
    }
  }

  fn get_retry_count(delivery: &Result<Delivery>) -> i64 {
    if let Ok(delivery) = delivery {
      let headers = delivery.properties.headers();

      if let Some(headers) = headers {
        let x_death = headers.inner().get("x-death");

        // TODO: THere are too many unwraps here which we don't know if they are safe enough
        // As long as x_death exist the rest should be present. But we still need to monitor this
        if let Some(x_death) = x_death {
          return x_death
          .as_array()
          .unwrap()
          .as_slice()[0]
          .as_field_table()
          .unwrap()
          .inner()
          .get("count")
          .unwrap()
          .as_long_long_int()
          .unwrap_or(1)
        }
      }
    }

    1
  }
}
