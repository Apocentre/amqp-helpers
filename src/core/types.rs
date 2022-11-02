use async_trait::async_trait;
use borsh::{BorshDeserialize};
use lapin::{
	Result,
	message::{Delivery},
};

pub type MessageHandler<F> = Box<dyn Fn(Result<Delivery>, i64) -> F + Send + Sync + 'static>;

#[async_trait]
pub trait Handler<M: BorshDeserialize + Send + Sync> {
  async fn handle(&self, model: M, delivery: &Delivery) -> eyre::Result<()>;
}
