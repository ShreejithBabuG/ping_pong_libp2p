use crate::{interface::*, protocol::*, types::*};
use futures::{StreamExt, AsyncWriteExt};
use libp2p::{Multiaddr, PeerId, Swarm, Stream};
use libp2p_stream as stream;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct LibP2PNetwork {
    next_message_id: AtomicU64,
    local_peer_id: PeerId,
    local_addrs: Arc<tokio::sync::Mutex<Vec<GlobalAddress>>>,
    command_tx: mpsc::UnboundedSender<NetworkCommand>,
}

enum NetworkCommand {
    Send {
        id: MessageId,
        addr: GlobalAddress,
        msg: MeerkatMessage,
    },
    Listen {
        addr: GlobalAddress,
        response_tx: tokio::sync::oneshot::Sender<Result<GlobalAddress, NetworkError>>,
    },
}

impl LibP2PNetwork {
    pub fn new(callbacks: NetworkCallbacks) -> anyhow::Result<Self> {
        let swarm = libp2p::SwarmBuilder::with_new_identity()
            .with_tokio()
            .with_tcp(
                libp2p::tcp::Config::default(),
                libp2p::noise::Config::new,
                libp2p::yamux::Config::default,
            )?
            .with_behaviour(|_| stream::Behaviour::new())?
            .with_swarm_config(|c: libp2p::swarm::Config| {
                c.with_idle_connection_timeout(std::time::Duration::from_secs(60))
            })
            .build();

        let local_peer_id = *swarm.local_peer_id();
        let local_addrs = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        
        let local_addrs_clone = local_addrs.clone();
        tokio::spawn(async move {
            Self::event_loop(swarm, callbacks, command_rx, local_addrs_clone).await;
        });
        
        Ok(Self {
            next_message_id: AtomicU64::new(1),
            local_peer_id,
            local_addrs,
            command_tx,
        })
    }
    
    async fn event_loop(
        mut swarm: Swarm<stream::Behaviour>,
        callbacks: NetworkCallbacks,
        mut command_rx: mpsc::UnboundedReceiver<NetworkCommand>,
        local_addrs: Arc<tokio::sync::Mutex<Vec<GlobalAddress>>>,
    ) {
        let mut control = swarm.behaviour().new_control();
        let mut pending_sends: HashMap<PeerId, Vec<(MessageId, MeerkatMessage)>> = HashMap::new();
        let mut pending_listen: Option<tokio::sync::oneshot::Sender<Result<GlobalAddress, NetworkError>>> = None;
        
        let mut incoming = control.accept(MEERKAT_PROTOCOL).unwrap();
        
        loop {
            tokio::select! {
                Some(cmd) = command_rx.recv() => {
                    match cmd {
                        NetworkCommand::Send { id, addr, msg } => {
                            Self::handle_send(
                                &mut swarm,
                                &mut control,
                                &mut pending_sends,
                                id,
                                addr,
                                msg,
                                &callbacks,
                            ).await;
                        }
                        NetworkCommand::Listen { addr, response_tx } => {
                            match Self::parse_global_address(&addr) {
                                Ok(multiaddr) => {
                                    if let Err(e) = swarm.listen_on(multiaddr) {
                                        let _ = response_tx.send(Err(NetworkError::BindFailed(format!("{:?}", e))));
                                    } else {
                                        // Store response channel - will reply when we get NewListenAddr event
                                        pending_listen = Some(response_tx);
                                    }
                                }
                                Err(e) => {
                                    let _ = response_tx.send(Err(e));
                                }
                            }
                        }
                    }
                }
                
                Some((peer, stream)) = incoming.next() => {
                    let callbacks = callbacks.clone();
                    tokio::spawn(async move {
                        Self::handle_incoming_stream(peer, stream, callbacks).await;
                    });
                }
                
                event = swarm.next() => {
                    if let Some(event) = event {
                        Self::handle_swarm_event(
                            event,
                            &callbacks,
                            &mut control,
                            &mut pending_sends,
                            &local_addrs,
                            &mut pending_listen,
                        ).await;
                    }
                }
            }
        }
    }
    
    async fn handle_send(
        swarm: &mut Swarm<stream::Behaviour>,
        control: &mut stream::Control,
        pending_sends: &mut HashMap<PeerId, Vec<(MessageId, MeerkatMessage)>>,
        msg_id: MessageId,
        addr: GlobalAddress,
        msg: MeerkatMessage,
        callbacks: &NetworkCallbacks,
    ) {
        let multiaddr = match Self::parse_global_address(&addr) {
            Ok(addr) => addr,
            Err(_) => {
                (callbacks.on_send_error)(msg_id, SendError::UnreachableAddress(addr));
                return;
            }
        };
        
        let peer_id = match Self::extract_peer_id(&multiaddr) {
            Some(id) => id,
            None => {
                (callbacks.on_send_error)(
                    msg_id,
                    SendError::ProtocolError("No peer ID in address".to_string()),
                );
                return;
            }
        };
        
        if swarm.is_connected(&peer_id) {
            Self::send_to_peer(control, peer_id, msg_id, msg, callbacks).await;
        } else {
            pending_sends.entry(peer_id).or_default().push((msg_id, msg));
            
            if let Err(e) = swarm.dial(multiaddr) {
                (callbacks.on_send_error)(
                    msg_id,
                    SendError::ProtocolError(format!("Dial failed: {:?}", e)),
                );
                pending_sends.remove(&peer_id);
            }
        }
    }
    
    async fn send_to_peer(
        control: &mut stream::Control,
        peer: PeerId,
        msg_id: MessageId,
        msg: MeerkatMessage,
        callbacks: &NetworkCallbacks,
    ) {
        match control.open_stream(peer, MEERKAT_PROTOCOL).await {
            Ok(mut stream) => {
                if let Err(e) = send_message(&mut stream, &msg).await {
                    (callbacks.on_send_error)(
                        msg_id,
                        SendError::ProtocolError(format!("Send failed: {}", e)),
                    );
                } else {
                    let _ = stream.close().await;
                }
            }
            Err(e) => {
                (callbacks.on_send_error)(
                    msg_id,
                    SendError::ProtocolError(format!("Stream open failed: {:?}", e)),
                );
            }
        }
    }
    
    async fn handle_incoming_stream(
        peer: PeerId,
        mut stream: Stream,
        callbacks: NetworkCallbacks,
    ) {
        match recv_message(&mut stream).await {
            Ok(msg) => {
                (callbacks.on_message)(peer.to_string(), msg);
            }
            Err(e) => {
                eprintln!("Failed to receive message from {}: {}", peer, e);
            }
        }
        let _ = stream.close().await;
    }
    
    async fn handle_swarm_event(
        event: libp2p::swarm::SwarmEvent<()>,
        callbacks: &NetworkCallbacks,
        control: &mut stream::Control,
        pending_sends: &mut HashMap<PeerId, Vec<(MessageId, MeerkatMessage)>>,
        local_addrs: &Arc<tokio::sync::Mutex<Vec<GlobalAddress>>>,
        pending_listen: &mut Option<tokio::sync::oneshot::Sender<Result<GlobalAddress, NetworkError>>>,
    ) {
        match event {
            libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } => {
                let global_addr = GlobalAddress(address.to_string());
                local_addrs.lock().await.push(global_addr.clone());
                
                // Reply to pending listen request with actual address
                if let Some(tx) = pending_listen.take() {
                    let _ = tx.send(Ok(global_addr));
                }
            }
            libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                if let Some(ref cb) = callbacks.on_peer_connected {
                    cb(peer_id.to_string());
                }
                
                if let Some(messages) = pending_sends.remove(&peer_id) {
                    for (msg_id, msg) in messages {
                        Self::send_to_peer(control, peer_id, msg_id, msg, callbacks).await;
                    }
                }
            }
            libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, .. } => {
                if let Some(ref cb) = callbacks.on_peer_disconnected {
                    cb(peer_id.to_string());
                }
            }
            _ => {}
        }
    }
    
    fn parse_global_address(addr: &GlobalAddress) -> Result<Multiaddr, NetworkError> {
        addr.0
            .parse()
            .map_err(|_| NetworkError::InvalidAddress(addr.0.clone()))
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
}

#[async_trait::async_trait]
impl NetworkLayer for LibP2PNetwork {
    fn send(&mut self, addr: GlobalAddress, msg: MeerkatMessage) -> MessageId {
        let id = MessageId(self.next_message_id.fetch_add(1, Ordering::SeqCst));
        let _ = self.command_tx.send(NetworkCommand::Send { id, addr, msg });
        id
    }
    
    async fn listen(&mut self, addr: GlobalAddress) -> Result<(), NetworkError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        
        self.command_tx
            .send(NetworkCommand::Listen { addr, response_tx: tx })
            .map_err(|_| NetworkError::BindFailed("Channel closed".to_string()))?;
        
        // Wait for actual listen address
        rx.await
            .map_err(|_| NetworkError::BindFailed("Response channel closed".to_string()))?
            .map(|_| ()) // Discard the actual address, just return Ok
    }
    
    async fn local_addresses(&self) -> Vec<GlobalAddress> {
        self.local_addrs.lock().await.clone()
    }
    
    fn local_peer_id(&self) -> String {
        self.local_peer_id.to_string()
    }
}

impl Clone for NetworkCallbacks {
    fn clone(&self) -> Self {
        Self {
            on_message: self.on_message.clone(),
            on_send_error: self.on_send_error.clone(),
            on_peer_connected: self.on_peer_connected.clone(),
            on_peer_disconnected: self.on_peer_disconnected.clone(),
        }
    }
}
