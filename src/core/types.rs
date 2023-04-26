use async_trait::async_trait;
use borsh::{BorshDeserialize};
use lapin::{
	Result,
	message::{Delivery},
};

pub type MessageHandler<F> = Box<dyn FnMut(Result<Delivery>, i64) -> F + Send>;

#[async_trait]
pub trait Handler<M: BorshDeserialize + Send + Sync> {
  async fn handle(&mut self, model: M, delivery: &Delivery, retry_count: i64) -> eyre::Result<()>;
}
