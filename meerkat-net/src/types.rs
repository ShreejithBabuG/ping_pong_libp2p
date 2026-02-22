use serde::{Deserialize, Serialize};
use std::fmt;

/// Unique identifier for sent messages (for error tracking)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageId(pub u64);

/// Global address - how to reach a peer from the public internet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GlobalAddress(pub String);

impl GlobalAddress {
    pub fn new(addr: impl Into<String>) -> Self {
        GlobalAddress(addr.into())
    }
}

impl fmt::Display for GlobalAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Message types in the Meerkat protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MeerkatMessage {
    /// Ping for testing
    Ping { content: String },
    
    /// Pong response
    Pong { content: String },
    
    /// Peer announcement with their global address
    Announce { peer_addr: GlobalAddress },
}

/// Errors that can occur when sending
#[derive(Debug, Clone)]
pub enum SendError {
    /// Could not resolve/reach the address
    UnreachableAddress(GlobalAddress),
    
    /// Connection dropped before send completed
    ConnectionLost(String),
    
    /// Message too large or other protocol error
    ProtocolError(String),
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::UnreachableAddress(addr) => write!(f, "Unreachable: {}", addr),
            SendError::ConnectionLost(peer) => write!(f, "Connection lost: {}", peer),
            SendError::ProtocolError(msg) => write!(f, "Protocol error: {}", msg),
        }
    }
}

impl std::error::Error for SendError {}
