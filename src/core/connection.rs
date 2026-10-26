use eyre::{Result, WrapErr};
use futures_lite::StreamExt;
use log::{debug, error, info, warn};
use lapin::{
  Channel, Connection as LapinConnection, ConnectionProperties, Error, Event
};

pub struct Connection(pub LapinConnection);

impl Connection {
  pub async fn new<E>(uri: &str, mut error_handler: Option<E>) -> Result<Self>
  where
    E: FnMut(Error) + Send + 'static
  {
    let options = ConnectionProperties::default()
    .enable_auto_recover()
    .configure_backoff(|backoff| {
      backoff.with_max_times(3)
    });

    // the uri is deliberately left out of the error message because it usually contains credentials
    debug!("Connecting to rabbitmq");

    let connection = LapinConnection::connect(uri, options).await.wrap_err("cannot connect to rabbitmq")?;
    let mut events_listener = LapinConnection::events_listener(&connection);

    info!("Connected to rabbitmq");

    tokio::spawn(async move {
      // Keep listening after non-error events (e.g. `Connected` is also emitted after every recovery),
      // otherwise errors would stop being reported after the first one.
      while let Some(event) = events_listener.next().await {
        match event {
          Event::Connected => info!("Rabbitmq connection established (or re-established after recovery)"),
          Event::ConnectionBlocked(reason) => warn!("Rabbitmq blocked the connection: {}", reason),
          Event::ConnectionUnblocked => info!("Rabbitmq unblocked the connection"),
          Event::SendFlow(active) => warn!("Rabbitmq changed the flow, publishing is permitted: {}", active),
          Event::Error(error) => {
            error!("Rabbitmq connection error: {:?}", error);

            if let Some(h) = error_handler.as_mut() {
              h(error);
            }
          }
          // `Event` is non-exhaustive
          event => debug!("Unhandled rabbitmq connection event: {:?}", event),
        }
      }

      debug!("Rabbitmq connection events stream ended");
    });

    Ok(Self(connection))
  }

  pub async fn create_channel(&self) -> Result<Channel> {
    let channel = self.0.create_channel().await.wrap_err("cannot create channel")?;

    debug!("Created channel {}", channel.id());

    Ok(channel)
  }
}
