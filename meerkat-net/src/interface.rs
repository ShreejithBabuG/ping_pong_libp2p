use crate::types::*;
use std::sync::Arc;

/// Callbacks the manager provides to the net module
pub struct NetworkCallbacks {
    /// Called when a message arrives from a peer
    pub on_message: Arc<dyn Fn(String, MeerkatMessage) + Send + Sync>,
    
    /// Called when a message fails to send
    pub on_send_error: Arc<dyn Fn(MessageId, SendError) + Send + Sync>,
    
    /// Called when a peer connects (optional)
    pub on_peer_connected: Option<Arc<dyn Fn(String) + Send + Sync>>,
    
    /// Called when a peer disconnects (optional)
    pub on_peer_disconnected: Option<Arc<dyn Fn(String) + Send + Sync>>,
}

/// Errors from network operations
#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("Invalid address: {0}")]
    InvalidAddress(String),
    
    #[error("Bind failed: {0}")]
    BindFailed(String),
    
    #[error("Already listening")]
    AlreadyListening,
}

/// The net module interface - hides libp2p details
#[async_trait::async_trait]
pub trait NetworkLayer: Send {
    /// Send a message to a global address
    /// Returns immediately with MessageId for tracking
    /// Errors reported via on_send_error callback
    fn send(&mut self, addr: GlobalAddress, msg: MeerkatMessage) -> MessageId;
    
    /// Start listening on a global address (async)
    async fn listen(&mut self, addr: GlobalAddress) -> Result<(), NetworkError>;
    
    /// Get our own global address(es) (async)
    async fn local_addresses(&self) -> Vec<GlobalAddress>;
    
    /// Get local peer ID
    fn local_peer_id(&self) -> String;
}
