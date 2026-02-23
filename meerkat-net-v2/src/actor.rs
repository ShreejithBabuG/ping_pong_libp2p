use crate::{messages::*, types::*};
use kameo::Actor;
use libp2p::{Multiaddr, PeerId};
use libp2p_stream as stream;
use futures::StreamExt;
use futures::AsyncWriteExt;
use libp2p::Stream;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;

enum SwarmCommand {
    Send {
        id: MessageId,
        addr: Address,
        msg: MeerkatMessage,
    },
    Listen {
        addr: Address,
        reply_tx: tokio::sync::oneshot::Sender<Result<Address, String>>,
    },
}

#[derive(Actor)]
pub struct NetworkActor {
    next_message_id: AtomicU64,
    local_peer_id: PeerId,
    local_addrs: Vec<Address>,
    node_type: NodeType,
    command_tx: mpsc::UnboundedSender<SwarmCommand>,
    pub event_rx: mpsc::UnboundedReceiver<NetworkEvent>,
}

#[cfg(not(target_arch = "wasm32"))]
async fn build_swarm() -> anyhow::Result<(libp2p::Swarm<stream::Behaviour>, PeerId)> {
    let swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_websocket(
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )
        .await?
        .with_behaviour(|_| stream::Behaviour::new())?
        .with_swarm_config(|c: libp2p::swarm::Config| {
            c.with_idle_connection_timeout(std::time::Duration::from_secs(60))
        })
        .build();
    let peer_id = *swarm.local_peer_id();
    Ok((swarm, peer_id))
}

#[cfg(target_arch = "wasm32")]
async fn build_swarm() -> anyhow::Result<(libp2p::Swarm<stream::Behaviour>, PeerId)> {
    use libp2p::{core::upgrade, identity, Transport};

    let id_keys = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(id_keys.public());

    let transport = libp2p::websocket_websys::Transport::default()
        .upgrade(upgrade::Version::V1)
        .authenticate(libp2p::noise::Config::new(&id_keys)?)
        .multiplex(libp2p::yamux::Config::default())
        .boxed();

    let swarm = libp2p::Swarm::new(
        transport,
        stream::Behaviour::new(),
        local_peer_id,
        libp2p::swarm::Config::with_wasm_executor(),
    );

    Ok((swarm, local_peer_id))
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_event_loop(fut: impl std::future::Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

#[cfg(target_arch = "wasm32")]
fn spawn_event_loop(fut: impl std::future::Future<Output = ()> + 'static) {
    wasm_bindgen_futures::spawn_local(fut);
}

const MEERKAT_PROTOCOL: libp2p::StreamProtocol =
    libp2p::StreamProtocol::new("/meerkat/1.0.0");

impl NetworkActor {
    pub async fn new(node_type: NodeType) -> anyhow::Result<Self> {
        let (swarm, local_peer_id) = build_swarm().await?;

        let (command_tx, command_rx) = mpsc::unbounded_channel::<SwarmCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<NetworkEvent>();

        spawn_event_loop(Self::event_loop(swarm, command_rx, event_tx));

        Ok(Self {
            next_message_id: AtomicU64::new(1),
            local_peer_id,
            local_addrs: Vec::new(),
            node_type,
            command_tx,
            event_rx,
        })
    }

    pub fn local_peer_id(&self) -> String {
        self.local_peer_id.to_string()
    }

    pub async fn handle_command(&mut self, cmd: NetworkCommand) -> NetworkReply {
        match cmd {
            NetworkCommand::SendMessage { addr, msg } => {
                let msg_id = MessageId(
                    self.next_message_id.fetch_add(1, Ordering::SeqCst)
                );
                // Translate canonical address to local view before sending
                let local_addr = match self.translate_address(&addr) {
                    Ok(a) => a,
                    Err(e) => return NetworkReply::Failure(e.to_string()),
                };
                let _ = self.command_tx.send(SwarmCommand::Send {
                    id: msg_id,
                    addr: local_addr,
                    msg,
                });
                NetworkReply::MessageSent { msg_id }
            }
            NetworkCommand::Listen { addr } => {
                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                let _ = self.command_tx.send(SwarmCommand::Listen {
                    addr,
                    reply_tx,
                });
                match reply_rx.await {
                    Ok(Ok(actual_addr)) => {
                        self.local_addrs.push(actual_addr.clone());
                        NetworkReply::ListenSuccess { addr: actual_addr }
                    }
                    Ok(Err(e)) => NetworkReply::Failure(e),
                    Err(_) => NetworkReply::Failure("Event loop dropped".to_string()),
                }
            }
            NetworkCommand::GetLocalAddresses => {
                NetworkReply::LocalAddresses {
                    addrs: self.local_addrs.clone(),
                }
            }
        }
    }

    /// Translate canonical Address to local Multiaddr.
    ///
    /// Rules (from Jonathan's design):
    /// - Server node: use canonical address directly — servers can dial IP
    /// - Browser client: if canonical starts with /ip4/ (not directly dialable),
    ///   prepend our relay server's WebSocket address + /p2p-circuit/
    ///
    /// Example:
    ///   canonical:   /ip4/server2/tcp/9000/p2p/server2-id/p2p-circuit/p2p/client2-id
    ///   local view:  /ip4/server1/tcp/9001/ws/p2p/server1-id/p2p-circuit/
    ///                    /ip4/server2/tcp/9000/p2p/server2-id/p2p-circuit/p2p/client2-id
    fn translate_address(&self, canonical: &Address) -> anyhow::Result<Address> {
        match &self.node_type {
            // Servers use canonical address directly
            NodeType::Server => Ok(canonical.clone()),

            // Browser clients prepend relay server hop if address starts with /ip4/
            NodeType::BrowserClient { relay_server } => {
                if canonical.0.starts_with("/ip4/") {
                    // Prepend: <relay_server>/p2p-circuit/<canonical>
                    let local = format!(
                        "{}/p2p-circuit/{}",
                        relay_server.0,
                        canonical.0
                    );
                    Ok(Address::new(local))
                } else {
                    // Already a WS or relay address, use as-is
                    Ok(canonical.clone())
                }
            }
        }
    }
}

impl NetworkActor {
    async fn event_loop(
        mut swarm: libp2p::Swarm<stream::Behaviour>,
        mut command_rx: mpsc::UnboundedReceiver<SwarmCommand>,
        event_tx: mpsc::UnboundedSender<NetworkEvent>,
    ) {
        let mut control = swarm.behaviour().new_control();
        let mut incoming = control.accept(MEERKAT_PROTOCOL).unwrap();
        let mut pending_sends: HashMap<PeerId, Vec<(MessageId, MeerkatMessage)>> = HashMap::new();
        let mut pending_listen: Option<tokio::sync::oneshot::Sender<Result<Address, String>>> = None;

        loop {
            tokio::select! {
                Some(cmd) = command_rx.recv() => {
                    match cmd {
                        SwarmCommand::Send { id, addr, msg } => {
                            Self::do_send(
                                &mut swarm,
                                &mut control,
                                &mut pending_sends,
                                &event_tx,
                                id,
                                addr,
                                msg,
                            ).await;
                        }
                        SwarmCommand::Listen { addr, reply_tx } => {
                            match addr.0.parse::<Multiaddr>() {
                                Ok(multiaddr) => {
                                    if let Err(e) = swarm.listen_on(multiaddr) {
                                        let _ = reply_tx.send(Err(format!("{:?}", e)));
                                    } else {
                                        pending_listen = Some(reply_tx);
                                    }
                                }
                                Err(e) => {
                                    let _ = reply_tx.send(Err(format!("Invalid address: {}", e)));
                                }
                            }
                        }
                    }
                }

                Some((peer, mut stream)) = incoming.next() => {
                    let event_tx = event_tx.clone();
                    tokio::spawn(async move {
                        Self::handle_incoming(peer, &mut stream, event_tx).await;
                        let _ = stream.close().await;
                    });
                }

                event = swarm.next() => {
                    if let Some(event) = event {
                        Self::handle_swarm_event(
                            event,
                            &mut control,
                            &mut pending_sends,
                            &event_tx,
                            &mut pending_listen,
                        ).await;
                    }
                }
            }
        }
    }

    async fn do_send(
        swarm: &mut libp2p::Swarm<stream::Behaviour>,
        control: &mut stream::Control,
        pending_sends: &mut HashMap<PeerId, Vec<(MessageId, MeerkatMessage)>>,
        event_tx: &mpsc::UnboundedSender<NetworkEvent>,
        msg_id: MessageId,
        addr: Address,
        msg: MeerkatMessage,
    ) {
        let multiaddr = match addr.0.parse::<Multiaddr>() {
            Ok(m) => m,
            Err(_) => {
                let _ = event_tx.send(NetworkEvent::SendFailed {
                    msg_id,
                    error: SendError::UnreachableAddress(addr),
                });
                return;
            }
        };

        let peer_id = match Self::extract_peer_id(&multiaddr) {
            Some(id) => id,
            None => {
                let _ = event_tx.send(NetworkEvent::SendFailed {
                    msg_id,
                    error: SendError::ProtocolError("No peer ID in address".to_string()),
                });
                return;
            }
        };

        if swarm.is_connected(&peer_id) {
            Self::send_to_peer(control, peer_id, msg_id, msg, event_tx).await;
        } else {
            pending_sends.entry(peer_id).or_default().push((msg_id, msg));
            if let Err(e) = swarm.dial(multiaddr) {
                let _ = event_tx.send(NetworkEvent::SendFailed {
                    msg_id,
                    error: SendError::ProtocolError(format!("Dial failed: {:?}", e)),
                });
                pending_sends.remove(&peer_id);
            }
        }
    }

    async fn send_to_peer(
        control: &mut stream::Control,
        peer: PeerId,
        msg_id: MessageId,
        msg: MeerkatMessage,
        event_tx: &mpsc::UnboundedSender<NetworkEvent>,
    ) {
        match control.open_stream(peer, MEERKAT_PROTOCOL).await {
            Ok(mut stream) => {
                let data = match serde_json::to_vec(&msg) {
                    Ok(d) => d,
                    Err(e) => {
                        let _ = event_tx.send(NetworkEvent::SendFailed {
                            msg_id,
                            error: SendError::ProtocolError(format!("Serialize: {}", e)),
                        });
                        return;
                    }
                };
                let len = (data.len() as u32).to_be_bytes();
                let ok = stream.write_all(&len).await.is_ok()
                    && stream.write_all(&data).await.is_ok();
                if !ok {
                    let _ = event_tx.send(NetworkEvent::SendFailed {
                        msg_id,
                        error: SendError::ProtocolError("Write failed".to_string()),
                    });
                }
                let _ = stream.close().await;
            }
            Err(e) => {
                let _ = event_tx.send(NetworkEvent::SendFailed {
                    msg_id,
                    error: SendError::ProtocolError(format!("Stream open: {:?}", e)),
                });
            }
        }
    }

    async fn handle_incoming(
        peer: PeerId,
        stream: &mut Stream,
        event_tx: mpsc::UnboundedSender<NetworkEvent>,
    ) {
        use futures::AsyncReadExt;
        let mut len_bytes = [0u8; 4];
        if stream.read_exact(&mut len_bytes).await.is_err() { return; }
        let len = u32::from_be_bytes(len_bytes) as usize;
        let mut data = vec![0u8; len];
        if stream.read_exact(&mut data).await.is_err() { return; }
        if let Ok(msg) = serde_json::from_slice::<MeerkatMessage>(&data) {
            let _ = event_tx.send(NetworkEvent::MessageReceived {
                peer: peer.to_string(),
                msg,
            });
        }
    }

    async fn handle_swarm_event(
        event: libp2p::swarm::SwarmEvent<()>,
        control: &mut stream::Control,
        pending_sends: &mut HashMap<PeerId, Vec<(MessageId, MeerkatMessage)>>,
        event_tx: &mpsc::UnboundedSender<NetworkEvent>,
        pending_listen: &mut Option<tokio::sync::oneshot::Sender<Result<Address, String>>>,
    ) {
        match event {
            libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } => {
                let addr = Address(address.to_string());
                if let Some(tx) = pending_listen.take() {
                    let _ = tx.send(Ok(addr));
                }
            }
            libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                let _ = event_tx.send(NetworkEvent::PeerConnected {
                    peer: peer_id.to_string(),
                });
                if let Some(messages) = pending_sends.remove(&peer_id) {
                    for (msg_id, msg) in messages {
                        Self::send_to_peer(control, peer_id, msg_id, msg, event_tx).await;
                    }
                }
            }
            libp2p::swarm::SwarmEvent::ConnectionClosed { peer_id, .. } => {
                let _ = event_tx.send(NetworkEvent::PeerDisconnected {
                    peer: peer_id.to_string(),
                });
            }
            _ => {}
        }
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

impl NetworkActor {
    pub fn translate_address_pub(&self, canonical: &Address) -> Address {
        self.translate_address(canonical).unwrap()
    }
}
