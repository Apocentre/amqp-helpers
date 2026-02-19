use async_trait::async_trait;
use serde::de::DeserializeOwned;
use lapin::{
	Result,
	message::Delivery,
};

pub type MessageHandler<F> = Box<dyn FnMut(Result<Delivery>, i64) -> F + Send>;

#[async_trait]
pub trait Handler<M: DeserializeOwned + Send + Sync> {
  async fn handle(&self, model: M, delivery: &Delivery, retry_count: i64) -> eyre::Result<()>;
}
