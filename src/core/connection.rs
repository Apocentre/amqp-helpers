use lapin::{
  Connection as LapinConnection,
  ConnectionProperties,
  Channel,
};

pub struct Connection(pub LapinConnection);

impl Connection {
  pub async fn new(uri: &str) -> Self {
    let options = ConnectionProperties::default()
      // Use tokio executor and reactor.
      // At the moment the reactor is only available for unix.
      .with_executor(tokio_executor_trait::Tokio::current())
      .with_reactor(tokio_reactor_trait::Tokio);

      Self(
        LapinConnection::connect(uri, options).await.expect("cannot connect to rabbitmq")
      )
  }

  pub async fn create_channel(&self) -> Channel {
    self.0.create_channel().await.expect("cannot create channel")
  }
}
