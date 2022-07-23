use lapin::{
	Result,
	message::{Delivery},
};

pub type MessageHandler<F> = Box<dyn Fn(Result<Delivery>) -> F>;
