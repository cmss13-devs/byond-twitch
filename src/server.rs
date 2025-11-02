use eyre::bail;
use futures::{stream::FusedStream, SinkExt};
use std::{net::SocketAddr, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{broadcast::Sender, Mutex},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::trace;

pub struct WebsocketServer {
    addr: SocketAddr,
    tx: Arc<Mutex<Sender<String>>>,
}

impl WebsocketServer {
    #[must_use]
    pub fn new(addr: SocketAddr, tx: Sender<String>) -> Self {
        WebsocketServer {
            addr,
            tx: Arc::new(Mutex::new(tx)),
        }
    }

    pub async fn run(&self) -> tokio::io::Result<()> {
        let listener = TcpListener::bind(self.addr).await?;

        trace!("WebSocket server listening on {}", self.addr);

        loop {
            let (stream, _) = listener.accept().await?;
            let mut receiver = self.tx.lock().await.subscribe();
            tokio::spawn(async move {
                let ws_stream = accept_async(stream).await;
                match ws_stream {
                    Ok(mut ws) => {
                        loop {
                            if ws.is_terminated() {
                                break;
                            }

                            if let Ok(message) = receiver.recv().await {
                                let _ = ws.send(Message::text(message)).await;
                            }
                        }
                        Ok(())
                    }
                    Err(e) => bail!("Failed to accept websocket: {}", e),
                }
            });
        }
    }
}
