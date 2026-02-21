use wasm_bindgen::prelude::*;
use web_sys::console;
use futures::{StreamExt, AsyncReadExt, AsyncWriteExt};
use libp2p::{Multiaddr, PeerId, Swarm, identity};
use libp2p::core::{upgrade, Transport};
use libp2p_stream as stream;
use meerkat_protocol::{PingMessage, PongMessage, PING_PROTOCOL};
use std::sync::Mutex;

static CONTROL: Mutex<Option<stream::Control>> = Mutex::new(None);
static PEER_ID: Mutex<Option<PeerId>> = Mutex::new(None);

#[wasm_bindgen(start)]
pub fn main() {
    console::log_1(&"Meerkat WASM Client Initializing...".into());
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
}

#[wasm_bindgen]
pub async fn connect_to_server(server_addr: String) -> Result<(), JsValue> {
    console::log_1(&format!("Connecting to: {}", server_addr).into());

    let addr: Multiaddr = server_addr
        .parse()
        .map_err(|e| JsValue::from_str(&format!("Invalid address: {}", e)))?;

    let remote_peer_id = extract_peer_id(&addr)
        .ok_or_else(|| JsValue::from_str("No peer ID in address"))?;

    console::log_1(&format!("Remote Peer ID: {}", remote_peer_id).into());

    let id_keys = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(id_keys.public());

    let transport = libp2p::websocket_websys::Transport::default()
        .upgrade(upgrade::Version::V1)
        .authenticate(libp2p::noise::Config::new(&id_keys).unwrap())
        .multiplex(libp2p::yamux::Config::default())
        .boxed();

    let behaviour = stream::Behaviour::new();

    let mut swarm: Swarm<stream::Behaviour> = Swarm::new(
        transport,
        behaviour,
        local_peer_id,
        libp2p::swarm::Config::with_wasm_executor(),
    );

    console::log_1(&"Swarm built".into());

    swarm.dial(addr.clone())
        .map_err(|e| JsValue::from_str(&format!("Dial error: {:?}", e)))?;

    let mut control = swarm.behaviour().new_control();

    console::log_1(&"Waiting for connection...".into());

    loop {
        match swarm.next().await {
            Some(libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                console::log_1(&format!("Connected to: {}", peer_id).into());
                if peer_id == remote_peer_id {
                    break;
                }
            }
            Some(libp2p::swarm::SwarmEvent::OutgoingConnectionError { error, .. }) => {
                return Err(JsValue::from_str(&format!("Connection failed: {:?}", error)));
            }
            _ => {}
        }
    }

    // Store peer ID and control for later use
    *PEER_ID.lock().unwrap() = Some(remote_peer_id);
    *CONTROL.lock().unwrap() = Some(control.clone());

    // Spawn swarm event loop
    wasm_bindgen_futures::spawn_local(async move {
        loop {
            swarm.next().await;
        }
    });

    Ok(())
}

#[wasm_bindgen]
pub async fn send_ping(message: String) -> Result<String, JsValue> {
    console::log_1(&format!("Sending: {}", message).into());

    let peer_id = PEER_ID.lock().unwrap()
        .ok_or_else(|| JsValue::from_str("Not connected"))?;

    let mut control = CONTROL.lock().unwrap()
        .clone()
        .ok_or_else(|| JsValue::from_str("No control handle"))?;

    let mut stream = control
        .open_stream(peer_id, PING_PROTOCOL)
        .await
        .map_err(|e| JsValue::from_str(&format!("Stream error: {:?}", e)))?;

    let ping = PingMessage { message };
    let request_data = serde_json::to_vec(&ping)
        .map_err(|e| JsValue::from_str(&format!("Serialize error: {}", e)))?;
    let request_len = (request_data.len() as u32).to_be_bytes();

    stream.write_all(&request_len).await
        .map_err(|e| JsValue::from_str(&format!("Write error: {}", e)))?;
    stream.write_all(&request_data).await
        .map_err(|e| JsValue::from_str(&format!("Write error: {}", e)))?;

    console::log_1(&"Waiting for pong...".into());

    let mut len_bytes = [0u8; 4];
    stream.read_exact(&mut len_bytes).await
        .map_err(|e| JsValue::from_str(&format!("Read error: {}", e)))?;
    let len = u32::from_be_bytes(len_bytes) as usize;

    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await
        .map_err(|e| JsValue::from_str(&format!("Read error: {}", e)))?;

    let pong: PongMessage = serde_json::from_slice(&buf)
        .map_err(|e| JsValue::from_str(&format!("Deserialize error: {}", e)))?;

    console::log_1(&format!("Received: {}", pong.message).into());

    stream.close().await.ok();

    Ok(pong.message)
}

fn extract_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|proto| {
        if let libp2p::multiaddr::Protocol::P2p(peer_id) = proto {
            Some(peer_id)
        } else {
            None
        }
    })
}
