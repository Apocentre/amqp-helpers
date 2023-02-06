use eyre::Result;
use lapin::{
  Channel,
  ExchangeKind,
  Queue,
  options::{
    ExchangeDeclareOptions,
    QueueDeclareOptions,
    QueueBindOptions,
    BasicPublishOptions,
  },
  BasicProperties,
  types::{FieldTable, LongString, AMQPValue},
};
use crate::{
  core::{
    connection::Connection,
  },
};

const ANY_MESSAGE: &str = "#";

pub struct RetryProducer {
  channel: Channel,
  delay_ms: Option<i32>,
}

impl RetryProducer {
  pub async fn new(
    uri: &str,
    exchange_name: &str,
    queue_name: &str,
    routing_key: &str,
    ttl: u16,
    delay_ms: Option<i32>,
  ) -> Result<Self> {
    let connection = Connection::new(uri).await;
    let channel = connection.create_channel().await;

    let retry_1_exchange_name = Self::get_retry_exchange_name(exchange_name, 1);
    let retry_2_exchange_name = Self::get_retry_exchange_name(exchange_name, 2);
    let wait_queue_name = Self::get_wait_queue_name(queue_name);

    let (main_ex_args, main_ex_kind) = if let Some(_) = delay_ms {
      let mut options = FieldTable::default();
      options.insert("x-delayed-type".into(), AMQPValue::LongString("direct".into()));
      (options, ExchangeKind::Custom("x-delayed-message".into()),)
    } else {
      (FieldTable::default(), ExchangeKind::Direct)
    };

    // the main exchange that will route the messages to the queue
    Self::create_exchange(&channel, exchange_name, main_ex_kind, main_ex_args).await?;
    // the retry DLX that will accept messages when the consumer rejects a message
    Self::create_exchange(&channel, &retry_1_exchange_name, ExchangeKind::Topic, FieldTable::default()).await?;
    // the retry DLX that will accept messages that will be ttl'ed from the temporary wait queue
    Self::create_exchange(&channel, &retry_2_exchange_name, ExchangeKind::Topic, FieldTable::default()).await?;

    // create the given queue and set the retry 1 exchange as its DLX
    let mut args = FieldTable::default();
    let dlx = Into::<LongString>::into(retry_1_exchange_name.clone());
    // https://www.rabbitmq.com/dlx.html
    args.insert("x-dead-letter-exchange".into(), dlx.into());
    let _main_queue = Self::create_queue(&channel, queue_name, args).await;

    // bind the original exchange to the main queue
    Self::queue_bind(&channel, exchange_name, queue_name, routing_key).await?;

    // bind the retry DLX exchange to the main queue so messages that need a retry will be routed to it
    // to be next picked up by the consumer for another processing attempt
    Self::queue_bind(&channel, &retry_2_exchange_name, queue_name, ANY_MESSAGE).await?;

    // create a new temporary wait queue where messaged that are rejected will sit for some short time
    // before they're routed via the DLX retry 2 to the main queue for a retry
    let mut args = FieldTable::default();
    let dlx = Into::<LongString>::into(retry_2_exchange_name);
    args.insert("x-dead-letter-exchange".into(), dlx.into());
    // https://www.rabbitmq.com/ttl.html
    args.insert("x-message-ttl".into(), ttl.into());
    let _wait_queue = Self::create_queue(&channel, &wait_queue_name, args).await;

    // finally, bind the wait queue to the retry DLX where messages that are rejected are sent back to the main queue
    Self::queue_bind(&channel, &retry_1_exchange_name, &wait_queue_name, ANY_MESSAGE).await?;

    Ok(Self {channel, delay_ms})
  }

  pub async fn publish(
    &self,
    exchange_name: &str,
    routing_key: &str,
    payload: &[u8],
  ) -> Result<()> {
    let basic_props = if let Some(delay_ms) = self.delay_ms {
      let mut headers = FieldTable::default();
      headers.insert("x-delay".into(), AMQPValue::LongInt(delay_ms));
      BasicProperties::default().with_headers(headers)
    } else {
      BasicProperties::default()
    };

    self.channel.basic_publish(
      exchange_name,
      routing_key,
      BasicPublishOptions::default(),
      payload,
      basic_props,
    )
    .await?
    .await?;

    Ok(())
  }

  async fn create_exchange(
    channel: &Channel,
    exchange_name: &str,
    exchange_kind: ExchangeKind,
    args: FieldTable,
  ) -> Result<()> {
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
      args,
    ).await?;

    Ok(())
  }

  async fn create_queue(
    channel: &Channel,
    queue_name: &str,
    args: FieldTable,
  ) -> Result<Queue> {
    let queue = channel.queue_declare(
      queue_name,
      QueueDeclareOptions {
        passive: false, // TODO: what does this field mean?
        durable: true,
        auto_delete: false,
        exclusive: false,
        nowait: true, // TODO: what does this field mean?
      },
      args,
    ).await?;

    Ok(queue)
  }

  async fn queue_bind(
    channel: &Channel,
    exchange_name: &str,
    queue_name: &str,
    routing_key: &str,
  ) -> Result<()> {
    channel.queue_bind(
      queue_name,
      exchange_name,
      routing_key,
      QueueBindOptions {
        nowait: true,
      },
      FieldTable::default(),
    ).await?;

    Ok(())
  }

  fn get_retry_exchange_name(exchange_name: &str, count: u8,) -> String {
    format!("{}.dlx_retry_{}", exchange_name, count)
  }

  fn get_wait_queue_name(queue_name: &str) -> String {
    format!("{}.wait_retry", queue_name)
  }
}
