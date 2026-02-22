use crate::{interface::*, types::*};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

pub struct MockNetwork {
    next_message_id: AtomicU64,
    local_id: String,
    local_addrs: Vec<GlobalAddress>,
    shared: Arc<Mutex<SharedMockState>>,
}

pub struct SharedMockState {
    pub peers: HashMap<String, MockPeerHandle>,
}

impl Default for SharedMockState {
    fn default() -> Self {
        Self {
            peers: HashMap::new(),
        }
    }
}

pub struct MockPeerHandle {
    pub on_message: Arc<dyn Fn(String, MeerkatMessage) + Send + Sync>,
}

impl MockNetwork {
    pub fn new(_callbacks: NetworkCallbacks, shared: Arc<Mutex<SharedMockState>>) -> Self {
        let local_id = format!("mock-{}", rand::random::<u32>());
        
        Self {
            next_message_id: AtomicU64::new(1),
            local_id,
            local_addrs: Vec::new(),
            shared,
        }
    }
}

#[async_trait::async_trait]
impl NetworkLayer for MockNetwork {
    fn send(&mut self, addr: GlobalAddress, msg: MeerkatMessage) -> MessageId {
        let id = MessageId(self.next_message_id.fetch_add(1, Ordering::SeqCst));
        
        let shared = self.shared.clone();
        let from = self.local_id.clone();
        let addr_str = addr.0.clone();
        
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            
            let state = shared.lock().unwrap();
            if let Some(peer) = state.peers.get(&addr_str) {
                (peer.on_message)(from, msg);
            }
        });
        
        id
    }
    
    async fn listen(&mut self, addr: GlobalAddress) -> Result<(), NetworkError> {
        let mut state = self.shared.lock().unwrap();
        
        if state.peers.contains_key(&addr.0) {
            return Err(NetworkError::AlreadyListening);
        }
        
        // MockNetwork doesn't store callbacks, so we can't register here
        // This is a limitation of the mock - in real usage callbacks would be stored
        
        self.local_addrs.push(addr);
        Ok(())
    }
    
    async fn local_addresses(&self) -> Vec<GlobalAddress> {
        self.local_addrs.clone()
    }
    
    fn local_peer_id(&self) -> String {
        self.local_id.clone()
    }
}
