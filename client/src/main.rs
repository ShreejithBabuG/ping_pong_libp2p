use anyhow::{Context, Result};
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::{multiaddr::Protocol, Multiaddr, PeerId, Stream};
use libp2p_stream as stream;
use meerkat_protocol::{PingMessage, PongMessage, PING_PROTOCOL};
use std::time::Duration;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()?,
        )
        .init();

    println!("\nMeerkat Client (libp2p)\n");

    let address: Multiaddr = std::env::args()
        .nth(1)
        .context("Usage: meerkat-client <server-multiaddr>")?
        .parse()?;

    let Some(Protocol::P2p(peer_id)) = address.iter().last() else {
        anyhow::bail!("Address must end with /p2p/<peer-id>");
    };

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|_| stream::Behaviour::new())?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    swarm.dial(address)?;

    let control = swarm.behaviour().new_control();

    // Spawn swarm event loop
    tokio::spawn(async move {
        loop {
            swarm.next().await;
        }
    });

    // Send pings
    send_pings(peer_id, control).await?;

    Ok(())
}

async fn send_pings(peer: PeerId, mut control: stream::Control) -> Result<()> {
    let mut counter = 0;

    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        counter += 1;

        let stream = match control.open_stream(peer, PING_PROTOCOL).await {
            Ok(stream) => stream,
            Err(error @ stream::OpenStreamError::UnsupportedProtocol(_)) => {
                tracing::info!(%peer, %error);
                return Ok(());
            }
            Err(error) => {
                tracing::debug!(%peer, %error);
                continue;
            }
        };

        if let Err(e) = send_ping(stream, counter).await {
            tracing::warn!(%peer, "Ping failed: {e}");
            continue;
        }

        tracing::info!(%peer, "Ping-Pong #{counter} complete")
    }
}

async fn send_ping(mut stream: Stream, counter: usize) -> Result<()> {
    let ping = PingMessage {
        message: format!("Hello from Meerkat client (ping #{})", counter),
    };

    tracing::info!("Sending: {}", ping.message);

    // Write length-prefixed ping
    let request_data = serde_json::to_vec(&ping)?;
    let request_len = (request_data.len() as u32).to_be_bytes();

    stream.write_all(&request_len).await?;
    stream.write_all(&request_data).await?;

    // Read length-prefixed pong
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let pong: PongMessage = serde_json::from_slice(&buf)?;
    tracing::info!("Received: {}", pong.message);

    stream.close().await?;

    Ok(())
}
