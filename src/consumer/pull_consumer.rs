use lapin::{
  message::{BasicGetMessage, Delivery}, options::BasicGetOptions, Channel, Error
};
use eyre::Result;
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
  queue_name: String,
}

impl PullConsumer {
  pub async fn new<E>(
    uri: &str,
    queue_name: &str,
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
    
    let queue_name = queue_name.to_owned();

    Ok(Self {
      connection,
      channel,
      queue_name,
    })
  }

  /// This is a usefull technique to allow the unacked messages being returns back to the ready state.
  /// The client of this lib can periodically call this function to re-establish the channel.
  pub async fn recreate_channel(&mut self) -> Result<()> {
    self.channel = self.connection.create_channel().await;

    Ok(())
  }
  
  pub async fn next(&self) -> Result<Option<NextItem>> {
    let basic_get_result = self.channel.basic_get(&self.queue_name, BasicGetOptions {no_ack: false}).await?;
    let Some(BasicGetMessage {delivery, ..}) = basic_get_result else {return Ok(None)};
    let delivery = Ok(delivery);
    let retry_count = RetryConsumer::get_retry_count(&delivery)?;

    let next_item = NextItem {
      delivery: delivery?,
      retry_count
    };
    
    Ok(Some(next_item))
  }

}
