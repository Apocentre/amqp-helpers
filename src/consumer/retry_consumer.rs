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

pub struct RetryConsumer;

static ANY_MESSAGE: &str = "#";

impl RetryConsumer {
  pub async fn new(
    uri: &str,
    exchange_name: &str,
    queue_name: &str,
    binding_key: &str,
    ttl: u64,
  ) -> Self {
    let connection = Connection::new(uri).await;
    let channel = connection.create_channel().await;

    let retry_1_exchange_name = Self::get_retry_exchange_name(exchange_name, 1);
    let retry_2_exchange_name = Self::get_retry_exchange_name(exchange_name, 2);
    let wait_queue_name = Self::get_wait_queue_name(queue_name);

    // the main exchange that will route the messages to the queue
    Self::create_exchange(&channel, exchange_name, ExchangeKind::Direct).await;
    // the retry DLX that will accept messages when the consumer rejects a message
    Self::create_exchange(&channel, &retry_1_exchange_name, ExchangeKind::Topic).await;
    // the retry DLX that will accept messages that will be ttl'ed from the temporary wait queue
    Self::create_exchange(&channel, &retry_2_exchange_name, ExchangeKind::Topic).await;

    // create the given queue and set the retry 1 exchange as its DLX
    let mut args = FieldTable::default();
    let dlx = Into::<ShortString>::into(retry_1_exchange_name.clone());
    // https://www.rabbitmq.com/dlx.html
    args.insert("x-dead-letter-exchange".into(), dlx.into());
    let _main_queue = Self::create_queue(&channel, queue_name, args).await;

    // bind to the original exchange to the main queue
    Self::queue_bind(&channel, exchange_name, queue_name, binding_key).await;

    // bind the retry DLX exchange to the main queue so messages that need a retry will be routed to it
    // to be next picked up by the consumer for another processing attempt
    Self::queue_bind(&channel, &retry_2_exchange_name, queue_name, ANY_MESSAGE).await;

    // create a new temporary wait queue where messaged that are rejected will sit for some short time
    // before they're routed via the DLX retry 2 to the main queue for a retry
    let mut args = FieldTable::default();
    let dlx = Into::<ShortString>::into(retry_2_exchange_name);
    args.insert("x-dead-letter-exchange".into(), dlx.into());
    // https://www.rabbitmq.com/ttl.html
    args.insert("x-message-ttl".into(), ttl.into());
    let _wait_queue = Self::create_queue(&channel, &wait_queue_name, args).await;

    // finally, bind the wait queue to the retry DLX where messages that are rejected are sent back to the main queue
    Self::queue_bind(&channel, &retry_1_exchange_name, &wait_queue_name, ANY_MESSAGE).await;
    
    Self {}
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

  fn get_retry_exchange_name(exchange_name: &str, count: u8,) -> String {
    format!("{}dlx_retry_{}", exchange_name, count)
  }

  fn get_wait_queue_name(queue_name: &str) -> String {
    format!("{}.wait_retry", queue_name)
  }
}
