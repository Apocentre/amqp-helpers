use lapin::{
  Channel,
  ExchangeKind,
  Queue,
  options::{
    ExchangeDeclareOptions,
    QueueDeclareOptions,
    QueueBindOptions,
  },
  types::{FieldTable, ShortString},
};
use crate::{
  core::connection::Connection,
};

pub struct RetryConsumer {
  channel: Channel,
}

impl RetryConsumer {
  pub async fn new(
    uri: &str,
    exchange_name: &str,
    queue_name: &str,
    binding_key: &str,
  ) -> Self {
    let connection = Connection::new(uri).await;
    let channel = connection.create_channel().await;

    // the main exchange that will route the messages to the queue
    Self::create_exchange(&channel, exchange_name, ExchangeKind::Direct).await;
    // the retry DLX that will accept messages when the consumer rejects a message
    Self::create_exchange(&channel, Self::get_retry_exchange_name(exchange_name, 1), ExchangeKind::Topic).await;
    // the retry DLX that will accept messages that will be ttl'ed from the temporary wait queue
    Self::create_exchange(&channel, Self::get_retry_exchange_name(exchange_name, 2), ExchangeKind::Topic).await;

    // create the given queue and set the retry 1 exchange as its DLX
    let mut args = FieldTable::default();
    let dlx = Into::<ShortString>::into(Self::get_retry_exchange_name(exchange_name, 1));
    args.insert("x-dead-letter-exchange".into(), dlx.into());
    let queue = Self::create_queue(&channel, queue_name, args).await;

    // bind to the original exchange to the main queue

    todo!()
  }

  async fn create_exchange(
    channel: &Channel,
    exchange_name: &str,
    exchange_kind: ExchangeKind,
  ) {
    channel.exchange_declare(
      exchange_name,
      exchange_kind,
      ExchangeDeclareOptions {
        passive: false, // TODO: what does this field mean?
        durable: true,
        auto_delete: true,
        internal: false,
        nowait: true, // TODO: what does this field mean?
      },
      FieldTable::default()
    ).await.expect(&format!("cannot create {} exchange", exchange_name));
  }

  async fn create_queue(
    channel: &Channel,
    queue_name: &str,
    args: FieldTable,
  ) -> Queue {
    channel.queue_declare(
      queue_name,
      QueueDeclareOptions {
        passive: false, // TODO: what does this field mean?
        durable: true,
        auto_delete: false,
        exclusive: false,
        nowait: true, // TODO: what does this field mean?
      },
      args,
    ).await.expect(&format!("cannot create {} queue", queue_name))
  }

  async fn queue_bind(
    channel: &Channel,
    exchange_name: &str,
    queue_name: &str,
    binding_key: &str,
  ) {
    channel.queue_bind(
      queue_name,
      exchange_name,
      binding_key,
      QueueBindOptions {
        nowait: true,
      },
      FieldTable::default(),
    ).await.expect(&format!("cannot bind {} queue to exchange {}", queue_name, exchange_name));
  }

  fn get_retry_exchange_name(exchange_name: &str, count: u8,) -> &str {
    &format!("{}dlx_retry_{}", exchange_name, count)
  }
}
