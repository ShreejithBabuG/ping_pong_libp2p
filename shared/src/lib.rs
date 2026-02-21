use libp2p::StreamProtocol;
use serde::{Deserialize, Serialize};

pub const PING_PROTOCOL: StreamProtocol = StreamProtocol::new("/meerkat-ping");

#[derive(Debug, Serialize, Deserialize)]
pub struct PingMessage {
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PongMessage {
    pub message: String,
}
