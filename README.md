# Meerkat libp2p

A peer-to-peer messaging layer for the Meerkat distributed runtime, built with Rust and libp2p. Supports native servers and WebAssembly browser clients.

This repository contains two components:
1. **Ping-Pong Demo** - Working example of libp2p messaging (server, native client, browser client)
2. **Meerkat-Net-V2** - Actor-based network abstraction layer (current development)

## Architecture
```
┌─────────────────────┐
│   Browser Client    │
│     (WebAssembly)   │
└──────────┬──────────┘
           │ WebSocket
           │ Port 9001
           ▼
┌─────────────────────┐        ┌─────────────────────┐
│   libp2p Server     │◄───────┤  Native Client      │
│   TCP + WebSocket   │  TCP   │     (Rust)          │
│   Ports: 9000,9001  │  9000  │                     │
└─────────────────────┘        └─────────────────────┘
           │
           │ NetworkActor (Kameo)
           ▼
    ┌──────────────────────┐
    │  MeerkatMessage      │
    │  Address             │
    │  NetworkEvent        │
    │  /meerkat/1.0.0      │
    └──────────────────────┘
```

## Meerkat-Net-V2: Actor-Based Network Layer

The `meerkat-net-v2` module is the current active development. It redesigns the network layer as a Kameo actor, replacing the previous callback-based approach.

### Design
```rust
// Create a server node
let mut net = NetworkActor::new(NodeType::Server).await?;

// Create a browser client node (routes through relay server)
let mut net = NetworkActor::new(NodeType::BrowserClient {
    relay_server: Address::new("/ip4/server-ip/tcp/9001/ws/p2p/server-id"),
}).await?;

// Send is non-blocking, returns MessageId immediately
let reply = net.handle_command(NetworkCommand::SendMessage {
    addr: Address::new("/ip4/127.0.0.1/tcp/9000/p2p/12D3KooW..."),
    msg: MeerkatMessage::Ping { content: "hello".to_string() },
}).await;

// Events arrive on event_rx
while let Ok(event) = net.event_rx.try_recv() {
    match event {
        NetworkEvent::MessageReceived { peer, msg } => { /* handle */ }
        NetworkEvent::SendFailed { msg_id, error } => { /* handle */ }
        NetworkEvent::PeerConnected { peer } => { /* handle */ }
        NetworkEvent::PeerDisconnected { peer } => { /* handle */ }
    }
}
```

### Key Components

- **`NetworkActor`** - Kameo actor wrapping the full libp2p swarm
- **`NetworkCommand`** - Messages sent TO the actor (SendMessage, Listen, GetLocalAddresses)
- **`NetworkReply`** - Replies FROM the actor (MessageSent, ListenSuccess, LocalAddresses, Failure)
- **`NetworkEvent`** - Async events fired by the actor (MessageReceived, SendFailed, PeerConnected, PeerDisconnected)
- **`MeerkatMessage`** - Typed message enum (Ping, Pong, Announce, Transaction, Propagation)
- **`Address`** - Canonical internet-routable address, serializable for inclusion in messages
- **`NodeType`** - Server or BrowserClient, controls address translation behavior

### Address Translation (Circuit Relay)

The network layer handles multi-hop routing transparently. The Manager only ever sees canonical addresses. For browser clients, the net layer internally prepends the relay server hop:
```
Canonical (what Manager sees):
/ip4/server2-ip/tcp/9000/p2p/server2-id/p2p-circuit/p2p/client2-id

Local view (internal, browser client only):
/ip4/server1-ip/tcp/9001/ws/p2p/server1-id/p2p-circuit/
    /ip4/server2-ip/tcp/9000/p2p/server2-id/p2p-circuit/p2p/client2-id
```

### Platform Support

| Target | Transport | Runtime |
|--------|-----------|---------|
| Native (server) | TCP + WebSocket | Tokio multi-thread |
| WASM (browser) | WebSocket (websys) | Tokio current-thread |

Both `cargo test` and `cargo build --target wasm32-unknown-unknown` pass cleanly.

### Testing
```bash
# Run all tests
cargo test -p meerkat-net-v2

# Check WASM build
cargo build -p meerkat-net-v2 --target wasm32-unknown-unknown
```

Tests cover: server-to-server messaging, multiple messages, server address translation (no-op), browser client address translation (relay prepend).

---

## Ping-Pong Demo (meerkat-net original)

The original callback-based implementation. Still useful as a working reference for the libp2p transport setup.

### Prerequisites

- Rust 1.70 or later
- wasm-pack: `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`
- Python 3 (for serving WASM)

### Run the Server
```bash
cargo run -p meerkat-server
```

Output:
```
Server ready
  TCP:       127.0.0.1:9000 (for native clients)
  WebSocket: 127.0.0.1:9001 (for browser clients)

Listening on: /ip4/127.0.0.1/tcp/9001/ws/p2p/12D3KooW...
Listening on: /ip4/127.0.0.1/tcp/9000/p2p/12D3KooW...
```

### Run the Native Client
```bash
cargo run -p meerkat-client -- /ip4/127.0.0.1/tcp/9000/p2p/12D3KooW...
```

### Run the Browser Client
```bash
cd wasm-client
wasm-pack build --target web
cp -r pkg www/
cd www
python3 -m http.server 8080
```

Open http://localhost:8080, paste the WebSocket address (port 9001 with `/ws`), connect and send pings.

---

## Project Structure
```
meerkat_libp2p/
├── meerkat-net-v2/      # Actor-based network layer (current)
│   ├── src/
│   │   ├── actor.rs        # NetworkActor + libp2p event loop
│   │   ├── messages.rs     # NetworkCommand, NetworkReply, NetworkEvent
│   │   ├── types.rs        # MeerkatMessage, Address, NodeType, SendError
│   │   └── lib.rs
│   └── tests/
│       └── integration_test.rs
├── meerkat-net/         # Original callback-based layer (reference)
├── shared/              # Ping-pong protocol types
├── server/              # Demo server (TCP + WebSocket)
├── client/              # Demo native client (TCP)
└── wasm-client/         # Demo browser client (WASM)
```

## Dependencies

- `libp2p` 0.56 — Peer-to-peer networking
- `libp2p-stream` 0.4.0-alpha — Custom stream protocol
- `kameo` 0.14 — Actor framework
- `tokio` — Async runtime
- `wasm-bindgen` — Rust/JavaScript interop (browser)
- `serde` + `serde_json` — Message serialization
