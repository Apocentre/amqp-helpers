use lapin::{
  message::{Delivery, BasicGetMessage}, Channel, options::BasicGetOptions
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
  queue_name: String,
}

impl PullConsumer {
  pub async fn new(
    uri: &str,
    queue_name: &str,
  ) -> Result<Self> {
    let connection = Connection::new(uri).await;
    let channel = connection.create_channel().await;
    let queue_name = queue_name.to_owned();

    Ok(Self {channel, queue_name})
  }
  
  pub async fn next(&mut self) -> Result<Option<NextItem>> {
    let basic_get_result = self.channel.basic_get(&self.queue_name, BasicGetOptions {no_ack: true}).await?;
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
