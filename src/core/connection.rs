use futures_lite::StreamExt;
use lapin::{
  Channel, Connection as LapinConnection, ConnectionProperties, Error, Event
};

pub struct Connection(pub LapinConnection);

impl Connection {
  pub async fn new<E>(uri: &str, mut error_handler: Option<E>) -> Self
  where
    E: FnMut(Error) + Send + 'static
  {
    let options = ConnectionProperties::default()
    .enable_auto_recover()
    .configure_backoff(|backoff| {
      backoff.with_max_times(3);
    });

    let connection = LapinConnection::connect(uri, options).await.expect("cannot connect to rabbitmq");
    let mut events_listener = LapinConnection::events_listener(&connection);

    tokio::spawn(async move {
      while let Some(Event::Error(error)) = events_listener.next().await {
        if let Some(ref mut h) = error_handler {
          h(error);
        }
      }
    });

    Self(connection)
  }

  pub async fn create_channel(&self) -> Channel {
    self.0.create_channel().await.expect("cannot create channel")
  }
}
