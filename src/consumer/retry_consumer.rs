use std::{
	future::Future
};
use futures_lite::stream::StreamExt;
use lapin::{
  Consumer,
  options::{
    BasicConsumeOptions,
  },
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
      handler(delivery).await;
    }
  }
}
