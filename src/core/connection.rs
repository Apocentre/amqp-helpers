use lapin::{
  Channel, Connection as LapinConnection, ConnectionProperties, Error
};

pub struct Connection(pub LapinConnection);

impl Connection {
  pub async fn new<E>(uri: &str, error_handler: Option<E>) -> Self
  where
    E: FnMut(Error) + Send + 'static
  {
    let options = ConnectionProperties::default()
    // Use tokio executor and reactor.
    // At the moment the reactor is only available for unix.
    .with_executor(tokio_executor_trait::Tokio::current())
    .with_reactor(tokio_reactor_trait::Tokio);

    let connection = LapinConnection::connect(uri, options).await.expect("cannot connect to rabbitmq");

    if let Some(h) = error_handler {
      connection.on_error(h);
    }

    Self(connection)
  }

  pub async fn create_channel(&self) -> Channel {
    self.0.create_channel().await.expect("cannot create channel")
  }
}
