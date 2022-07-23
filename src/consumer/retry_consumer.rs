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
    types::{DeliveryHandler, HandlerFn},
  },
};

pub struct RetryConsumer {
  consumer: Consumer,
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

  pub fn consume <F: HandlerFn>(&self, handler: DeliveryHandler<F>) {
    self.consumer.set_delegate(handler);
  }
}
