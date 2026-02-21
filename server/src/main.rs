use anyhow::Result;
use futures::{AsyncReadExt, AsyncWriteExt, StreamExt};
use libp2p::Stream;
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

    println!("\nMeerkat Server (libp2p)");
    println!("TCP + WebSocket Support\n");

    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_websocket(
            (libp2p::tls::Config::new, libp2p::noise::Config::new),
            libp2p::yamux::Config::default,
        )
        .await?
        .with_behaviour(|_| stream::Behaviour::new())?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();

    // Listen on TCP for native clients
    swarm.listen_on("/ip4/127.0.0.1/tcp/9000".parse()?)?;
    
    // Listen on WebSocket for browser clients
    swarm.listen_on("/ip4/127.0.0.1/tcp/9001/ws".parse()?)?;

    let mut incoming_streams = swarm
        .behaviour()
        .new_control()
        .accept(PING_PROTOCOL)
        .unwrap();

    // Handle incoming ping requests
    tokio::spawn(async move {
        while let Some((peer, stream)) = incoming_streams.next().await {
            tokio::spawn(async move {
                match handle_ping(stream).await {
                    Ok(_) => tracing::info!(%peer, "Pong sent"),
                    Err(e) => tracing::warn!(%peer, "Error: {e}"),
                }
            });
        }
    });

    println!("Server ready");
    println!("  TCP:       127.0.0.1:9000 (for native clients)");
    println!("  WebSocket: 127.0.0.1:9001 (for browser clients)\n");

    // Event loop
    loop {
        let event = swarm.next().await.expect("never terminates");

        match event {
            libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } => {
                let full_addr = address.with_p2p(*swarm.local_peer_id()).unwrap();
                tracing::info!("Listening on: {full_addr}");
            }
            libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                tracing::info!("Connected: {peer_id}");
            }
            _ => {}
        }
    }
}

async fn handle_ping(mut stream: Stream) -> Result<()> {
    // Read length-prefixed ping
    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;

    let ping: PingMessage = serde_json::from_slice(&buf)?;
    tracing::info!("Received: {}", ping.message);

    // Create pong
    let pong = PongMessage {
        message: format!("Pong! You said: {}", ping.message),
    };

    // Write length-prefixed pong
    let response_data = serde_json::to_vec(&pong)?;
    let response_len = (response_data.len() as u32).to_be_bytes();

    stream.write_all(&response_len).await?;
    stream.write_all(&response_data).await?;

    tracing::info!("Sent: {}", pong.message);

    Ok(())
}
