use std::{
	future::Future,
	pin::Pin,
};
use lapin::{
	message::{DeliveryResult},
	ConsumerDelegate,
};

struct DeliveryHandler<F>(
	Box<dyn Fn(DeliveryResult) -> F + Send + Sync + 'static>
);

// Inspired by this https://docs.rs/lapin/2.1.1/src/lapin/consumer.rs.html#35
impl<F: Future<Output = ()> + Send + 'static> ConsumerDelegate for DeliveryHandler<F> {
	fn on_new_delivery(&self, delivery: DeliveryResult) -> Pin<Box<dyn Future<Output = ()> + Send>> {
		Box::pin(self.0(delivery))
	}
}
