use eyre::Result;
use lapin::{
  BasicProperties, Channel, Error, ExchangeKind, Queue, options::{
    BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions
  }, types::{FieldTable, LongString}
};
use crate::core::connection::Connection;

const ANY_MESSAGE: &str = "#";

pub struct RetryProducer {
  channel: Channel,
  delay_ms: Option<u32>,
}

impl RetryProducer {
  pub async fn new<E>(
    uri: &str,
    exchange_name: &str,
    queue_name: &str,
    routing_key: &str,
    ttl: u32,
    delay_ms: Option<u32>,
    on_connection_error: Option<E>,
    on_channel_error: Option<E>,
  ) -> Result<Self>
  where
    E: FnMut(Error) + Send + 'static
  {
    let connection = Connection::new(uri, on_connection_error).await;
    let channel = connection.create_channel().await;

    if let Some(h) = on_channel_error {
      channel.on_error(h);
    }

    let retry_1_exchange_name = Self::get_retry_exchange_name(exchange_name, 1);
    let retry_2_exchange_name = Self::get_retry_exchange_name(exchange_name, 2);
    let wait_queue_name = format!("{}.wait_retry", queue_name);

    // the main exchange that will route the messages to the queue
    Self::create_exchange(&channel, exchange_name, ExchangeKind::Topic, FieldTable::default()).await?;
    // the retry DLX that will accept messages when the consumer rejects a message
    Self::create_exchange(&channel, &retry_1_exchange_name, ExchangeKind::Topic, FieldTable::default()).await?;
    // the retry DLX that will accept messages that will be ttl'ed from the temporary wait queue
    Self::create_exchange(&channel, &retry_2_exchange_name, ExchangeKind::Topic, FieldTable::default()).await?;

    let mut args = FieldTable::default();

    // create the delay exchange and queue if needed
    if let Some(delay_ms) = delay_ms {
      let delay_exchange_name = Self::get_delay_exchange_name(exchange_name);
      let delay_queue_name = format!("{}.entry_delay", queue_name);
      // the entry exchange that will receive all messages and will apply the delay logic
      Self::create_exchange(&channel, &delay_exchange_name, ExchangeKind::Direct, FieldTable::default()).await?;

      // make sure when message ttl'd from the delay queue it goes to the real exchange that will process the message
      let dlx = Into::<LongString>::into(exchange_name);
      args.insert("x-dead-letter-exchange".into(), dlx.into());
      args.insert("x-message-ttl".into(), delay_ms.into());
      Self::create_queue(&channel, &delay_queue_name, args).await?;

      // bind the entry exchange to the delay queue
      Self::queue_bind(&channel, &delay_exchange_name, &delay_queue_name, routing_key).await?;
    }

    // create the given queue and set the retry 1 exchange as its DLX
    let mut args = FieldTable::default();
    let dlx = Into::<LongString>::into(retry_1_exchange_name.clone());
    // https://www.rabbitmq.com/dlx.html
    args.insert("x-dead-letter-exchange".into(), dlx.into());
    Self::create_queue(&channel, queue_name, args).await?;

    // bind the original exchange to the main queue. Use ANY_MESSAGE because messages
    // can be published to the exchange_name when ttl'd through the delay queues (if applicable)
    Self::queue_bind(&channel, exchange_name, queue_name, ANY_MESSAGE).await?;

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
     Self::create_queue(&channel, &wait_queue_name, args).await?;

    // finally, bind the wait queue to the retry DLX where messages that are rejected are sent back to the main queue
    Self::queue_bind(&channel, &retry_1_exchange_name, &wait_queue_name, ANY_MESSAGE).await?;

    Ok(Self {channel, delay_ms})
  }

  pub async fn publish(
    &self,
    exchange_name: &str,
    routing_key: &str,
    payload: &[u8],
    persistent: bool,
    ttl: Option<u32>,
  ) -> Result<()> {
    let mut basic_props = BasicProperties::default();

    if persistent {
      basic_props = basic_props.with_delivery_mode(2);
    }

    let exchange_name = if let Some(_) = self.delay_ms {
      &Self::get_delay_exchange_name(exchange_name)
    } else {
      exchange_name
    };
    
    // per message ttl. When both a per-queue and a per-message TTL are specified,
    // the lower value between the two will be chosen.
    // https://www.rabbitmq.com/docs/ttl#per-message-ttl-in-publishers
    if let Some(ttl) = ttl {
      basic_props = basic_props.with_expiration(ttl.to_string().into());
    }

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
  
  fn get_delay_exchange_name(exchange_name: &str) -> String {
    format!("{}.delay_ex", exchange_name)
  }
}
