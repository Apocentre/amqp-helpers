use std::{collections::BTreeMap, fmt};
use eyre::Result;
use log::{debug, error, info, trace};
use lapin::{
  BasicProperties, Channel, Error, ExchangeKind, Queue, options::{
    BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions
  }, types::{AMQPValue, FieldTable, LongString, ShortString}
};
use crate::core::connection::Connection;

// delivery_mode in AMQP determines if message will be stored on disk after broker restarts.
const DURABLE_DELIVERY_MODE: u8 = 2;
const ANY_MESSAGE: &str = "#";

/// The broker received the message but refused it. The connection is healthy, so reconnecting does not help.
#[derive(Debug)]
pub enum PublishRejected {
  /// The broker nacked the message.
  Nacked {exchange_name: String, routing_key: String},
  /// No queue is bound for the routing key, so the broker returned the message.
  Unroutable {exchange_name: String, routing_key: String, reply_code: u16, reply_text: String},
}

impl fmt::Display for PublishRejected {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Self::Nacked {exchange_name, routing_key} => {
        write!(f, "broker nacked the message published to {exchange_name} with routing key {routing_key}")
      }
      Self::Unroutable {exchange_name, routing_key, reply_code, reply_text} => {
        write!(
          f,
          "message published to {exchange_name} with routing key {routing_key} is unroutable: {reply_code} {reply_text}",
        )
      }
    }
  }
}

impl std::error::Error for PublishRejected {}

pub struct RetryProducer {
  // kept so `close` can shut the connection down; lapin does not close it when the handle is dropped
  connection: Connection,
  channel: Channel,
  // `publish` always targets the exchange this producer declared the topology for
  exchange_name: String,
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
  ) -> Result<Self>
  where
    E: FnMut(Error) + Send + 'static
  {
    let connection = Connection::new(uri, on_connection_error).await?;
    let channel = connection.create_channel().await?;

    debug!("Declaring retry topology for exchange {} and queue {}", exchange_name, queue_name);

    // make the broker acknowledge every publish so `publish` can tell whether it took ownership of the message
    channel.confirm_select(ConfirmSelectOptions::default()).await?;

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

    info!("Declared retry topology for exchange {} and queue {} (delay_ms: {:?})", exchange_name, queue_name, delay_ms);

    Ok(Self {connection, channel, exchange_name: exchange_name.to_string(), delay_ms})
  }

  /// Publishes to the exchange given to `new` (or to its delay exchange when `delay_ms` was set).
  pub async fn publish(
    &self,
    routing_key: &str,
    payload: &[u8],
    persistent: bool,
    ttl: Option<u32>,
    headers: Option<BTreeMap<ShortString, AMQPValue>>,
  ) -> Result<()> {
    let mut basic_props = BasicProperties::default();

    if persistent {
      basic_props = basic_props.with_delivery_mode(DURABLE_DELIVERY_MODE);
    }

    if let Some(headers) = headers {
      basic_props = basic_props.with_headers(FieldTable::from(headers));
    }

    let exchange_name = if self.delay_ms.is_some() {
      Self::get_delay_exchange_name(&self.exchange_name)
    } else {
      self.exchange_name.clone()
    };

    // per message ttl. When both a per-queue and a per-message TTL are specified,
    // the lower value between the two will be chosen.
    // https://www.rabbitmq.com/docs/ttl#per-message-ttl-in-publishers
    if let Some(ttl) = ttl {
      basic_props = basic_props.with_expiration(ttl.to_string().into());
    }

    trace!("Publishing {} bytes to {} with routing key {}", payload.len(), exchange_name, routing_key);

    let confirmation = self.channel.basic_publish(
      exchange_name.as_str().into(),
      routing_key.into(),
      // mandatory: the broker returns the message instead of silently dropping it when no queue is bound
      BasicPublishOptions {mandatory: true, ..BasicPublishOptions::default()},
      payload,
      basic_props,
    )
    .await?
    .await?;

    if confirmation.is_nack() {
      error!("Broker nacked the message published to {} with routing key {}", exchange_name, routing_key);

      return Err(PublishRejected::Nacked {exchange_name, routing_key: routing_key.to_string()}.into());
    }

    if let Some(returned) = confirmation.take_message() {
      error!(
        "Message published to {} with routing key {} is unroutable: {} {}",
        exchange_name, routing_key, returned.reply_code, returned.reply_text,
      );

      return Err(PublishRejected::Unroutable {
        exchange_name,
        routing_key: routing_key.to_string(),
        reply_code: returned.reply_code,
        reply_text: returned.reply_text.to_string(),
      }.into());
    }

    trace!("Message published to {} with routing key {} was confirmed", exchange_name, routing_key);

    Ok(())
  }

  /// Whether the channel is still usable.
  pub fn is_connected(&self) -> bool {
    self.channel.status().connected()
  }

  /// Closes the connection (and with it the channel). Publishing afterwards fails.
  pub async fn close(&self) -> Result<()> {
    self.connection.0.close(200, "closing".into()).await?;

    Ok(())
  }

  async fn create_exchange(
    channel: &Channel,
    exchange_name: &str,
    exchange_kind: ExchangeKind,
    args: FieldTable,
  ) -> Result<()> {
    channel.exchange_declare(
      exchange_name.into(),
      exchange_kind,
      ExchangeDeclareOptions {
        passive: false, // false: create the exchange if missing instead of only checking that it exists
        durable: true,
        auto_delete: true,
        internal: false,
        // false: wait for the broker's reply so a failed declare (e.g. PRECONDITION_FAILED when the arguments
        // differ from the existing exchange) is returned here instead of asynchronously closing the channel
        nowait: false,
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
      queue_name.into(),
      QueueDeclareOptions {
        passive: false, // false: create the queue if missing instead of only checking that it exists
        durable: true,
        auto_delete: false,
        exclusive: false,
        nowait: false, // see create_exchange
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
      queue_name.into(),
      exchange_name.into(),
      routing_key.into(),
      QueueBindOptions {
        nowait: false, // see create_exchange
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
