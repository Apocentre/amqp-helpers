use lapin::{
  Channel, Error, message::{BasicGetMessage, Delivery}, options::BasicGetOptions, types::ShortString
};
use eyre::Result;
use log::{debug, info, trace};
use crate::core::connection::Connection;

use super::retry_consumer::RetryConsumer;

pub struct NextItem {
  pub delivery: Delivery,
  pub retry_count: i64,
}

/// Consumer using the (basic.get) Pull Api
pub struct PullConsumer {
  channel: Channel,
  connection: Connection,
  queue_name: ShortString,
}

impl PullConsumer {
  pub async fn new<E>(
    uri: &str,
    queue_name: &str,
    on_connection_error: Option<E>,
  ) -> Result<Self>
  where
    E: FnMut(Error) + Send + 'static
  {
    let connection = Connection::new(uri, on_connection_error).await?;
    let channel = connection.create_channel().await?;
    let queue_name = queue_name.into();

    info!("Pull consumer ready for queue {}", queue_name);

    Ok(Self {
      connection,
      channel,
      queue_name,
    })
  }

  /// This is a usefull technique to allow the unacked messages being returns back to the ready state.
  /// The client of this lib can periodically call this function to re-establish the channel.
  pub async fn recreate_channel(&mut self) -> Result<()> {
    self.channel = self.connection.create_channel().await?;

    debug!("Recreated channel for queue {}, unacked messages are returned to the queue", self.queue_name);

    Ok(())
  }

  pub async fn next(&self) -> Result<Option<NextItem>> {
    let basic_get_result = self.channel.basic_get(self.queue_name.clone(), BasicGetOptions {no_ack: false}).await?;

    let Some(BasicGetMessage {delivery, ..}) = basic_get_result else {
      trace!("Queue {} is empty", self.queue_name);

      return Ok(None)
    };

    let retry_count = RetryConsumer::get_retry_count(&delivery);
    trace!("Pulled delivery (delivery_tag: {}, retry_count: {})", delivery.delivery_tag, retry_count);

    Ok(Some(NextItem {delivery, retry_count}))
  }

}
