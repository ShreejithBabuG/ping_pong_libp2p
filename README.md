# Meerkat libp2p

A peer-to-peer messaging layer for the Meerkat distributed runtime, built with Rust and libp2p. Supports native servers and WebAssembly browser clients with circuit relay for browser-to-browser communication.

This repository contains two components:
1. **Ping-Pong Demo** - Working example of libp2p messaging (server, native client, browser client)
2. **Meerkat-Net-V2** - Actor-based network abstraction layer with circuit relay support

## Architecture
```
┌─────────────────────┐
│   Browser Client    │
│     (WebAssembly)   │
└──────────┬──────────┘
           │ WebSocket + Circuit Relay
           │ Port 9001
           ▼
┌─────────────────────┐        ┌─────────────────────┐
│   libp2p Server     │◄───────┤  Native Client      │
│   TCP + WebSocket   │  TCP   │     (Rust)          │
│   Relay Server      │  9000  │                     │
│   Ports: 9000,9001  │        │                     │
└─────────────────────┘        └─────────────────────┘
           │
           │ NetworkLayer trait
           ▼
    ┌──────────────────────┐
    │  MeerkatMessage      │
    │  Address             │
    │  NetworkEvent        │
    │  /meerkat/1.0.0      │
    └──────────────────────┘
```

## Meerkat-Net-V2: Actor-Based Network Layer

The `meerkat-net-v2` module provides a clean actor-based API for p2p messaging with automatic circuit relay for browser clients.

### Features

- Actor-based design using Kameo
- Circuit relay support for browser-to-browser communication
- Automatic address translation for multi-hop routing
- Mock network for testing without real libp2p
- Unified trait for real and mock implementations
- WASM compatible - runs in browsers
- 10/10 tests passing including full circuit relay test

### Quick Start
```rust
// Create a server node (acts as relay for browser clients)
let mut net = NetworkActor::new(NodeType::Server).await?;

// Listen on TCP and WebSocket
net.handle_command(NetworkCommand::Listen {
    addr: Address::new("/ip4/0.0.0.0/tcp/9000"),
}).await;

// Create a browser client node (routes through relay server)
let mut browser = NetworkActor::new(NodeType::BrowserClient {
    relay_server: Address::new("/ip4/server-ip/tcp/9001/ws/p2p/server-id"),
}).await?;

// Browser establishes circuit relay reservation
let circuit_addr = browser.handle_command(NetworkCommand::ListenViaRelay {
    relay_addr: relay_server_addr,
}).await;

// Send messages (non-blocking, returns MessageId immediately)
let reply = net.handle_command(NetworkCommand::SendMessage {
    addr: Address::new("/ip4/127.0.0.1/tcp/9000/p2p/12D3KooW..."),
    msg: MeerkatMessage::Ping { content: "hello".to_string() },
}).await;

// Events arrive via try_recv_event (NetworkLayer trait)
while let Some(event) = net.try_recv_event() {
    match event {
        NetworkEvent::MessageReceived { peer, msg } => { /* handle */ }
        NetworkEvent::SendFailed { msg_id, error } => { /* handle */ }
        NetworkEvent::PeerConnected { peer } => { /* handle */ }
        NetworkEvent::PeerDisconnected { peer } => { /* handle */ }
    }
}
```

### Circuit Relay: Browser-to-Browser Communication

Circuit relay enables browser clients (which can only use WebSocket) to communicate with each other through a relay server:
```
Browser A <-> Relay Server <-> Browser B
```

**How it works:**
1. Browser client connects to relay server via WebSocket
2. Client calls `ListenViaRelay` to establish a reservation
3. Relay server includes browser's address in identify protocol
4. Other clients can now dial the browser through the relay

**Address format:**
```
/ip4/relay-ip/tcp/9001/ws/p2p/relay-id/p2p-circuit/p2p/browser-id
                                       ^^^^^^^^^^^
                                       Circuit relay hop
```

The network layer handles address translation automatically - the application only sees canonical addresses.

### NetworkLayer Trait

Both `NetworkActor` and `MockNetwork` implement the same trait:
```rust
pub trait NetworkLayer {
    async fn handle_command(&mut self, cmd: NetworkCommand) -> NetworkReply;
    fn local_peer_id(&self) -> String;
    fn try_recv_event(&mut self) -> Option<NetworkEvent>;
}
```

This allows the Meerkat manager to work with either real or mock networks seamlessly.

### MockNetwork: Testing Without libp2p

For unit tests, use `MockNetwork` for instant message delivery in memory:
```rust
let registry = MockNetwork::new_registry();
let mut server = MockNetwork::new_with_registry(registry.clone());
let mut client = MockNetwork::new_with_registry(registry.clone());

// Listen
server.handle_command(NetworkCommand::Listen {
    addr: Address::new("/ip4/127.0.0.1/tcp/9000"),
}).await;

// Send - delivered instantly, no sleep needed
client.handle_command(NetworkCommand::SendMessage {
    addr: server_addr,
    msg: MeerkatMessage::Ping { content: "hello".to_string() },
}).await;

// Receive
let event = server.try_recv_event().unwrap();
assert!(matches!(event, NetworkEvent::MessageReceived { .. }));
```

### Key Components

- **`NetworkActor`** - Kameo actor wrapping the full libp2p swarm
- **`NetworkLayer`** - Trait implemented by both real and mock network
- **`MockNetwork`** - In-memory network for unit testing
- **`NetworkCommand`** - Messages sent TO the actor
  - `SendMessage` - Send a message to a peer
  - `Listen` - Start listening on an address
  - `ListenViaRelay` - Establish circuit relay reservation
  - `GetLocalAddresses` - Get all listening addresses
- **`NetworkReply`** - Replies FROM the actor
  - `MessageSent { msg_id }` - Message queued for delivery
  - `ListenSuccess { addr }` - Now listening on address
  - `LocalAddresses { addrs }` - List of listening addresses
  - `Failure(String)` - Operation failed
- **`NetworkEvent`** - Async events from the network
  - `MessageReceived { peer, msg }` - Incoming message
  - `SendFailed { msg_id, error }` - Send failed
  - `PeerConnected { peer }` - New peer connected
  - `PeerDisconnected { peer }` - Peer disconnected
- **`MeerkatMessage`** - Typed message enum (Ping, Pong, Announce, Transaction, Propagation)
- **`Address`** - Canonical internet-routable address, serializable
- **`NodeType`** - Server or BrowserClient, controls address translation

### libp2p Composite Behaviour

The network layer uses a composite behaviour with:
- **Stream** - Custom `/meerkat/1.0.0` protocol for messages
- **Relay Server** - Accept circuit relay reservations from browsers
- **Relay Client** - Make circuit relay reservations through other relays
- **Identify** - Exchange peer information and external addresses

### Address Translation (Transparent Multi-Hop)

The network layer handles circuit relay routing automatically. The Manager only sees canonical addresses:
```
Canonical (what Manager sees):
/ip4/server2-ip/tcp/9000/p2p/server2-id/p2p-circuit/p2p/client2-id

Local view (internal, browser client only):
/ip4/server1-ip/tcp/9001/ws/p2p/server1-id/p2p-circuit/
    /ip4/server2-ip/tcp/9000/p2p/server2-id/p2p-circuit/p2p/client2-id
```

**Translation rules:**
- **Server nodes**: No translation, use addresses as-is
- **Browser clients**: 
  - If address already uses our relay: no translation
  - If address is direct IP or uses different relay: prepend our relay hop

### Platform Support

| Target | Transport | Runtime | Relay |
|--------|-----------|---------|-------|
| Native (server) | TCP + WebSocket | Tokio multi-thread | Server & Client |
| WASM (browser) | WebSocket (websys) | Tokio current-thread | Client only |

Both `cargo test` and `cargo build --target wasm32-unknown-unknown` pass cleanly.

### Testing
```bash
# Run all tests (10/10 passing)
cargo test -p meerkat-net-v2

# Run specific test
cargo test -p meerkat-net-v2 test_circuit_relay -- --nocapture

# Check WASM build
cargo build -p meerkat-net-v2 --target wasm32-unknown-unknown
```

**Test Coverage:**
- Server-to-server messaging
- Multiple messages in sequence
- Address translation for servers
- Address translation for browser clients
- Mock send and receive
- Mock multiple messages
- Mock unreachable address
- NetworkLayer trait with mock
- NetworkLayer trait with real network
- Circuit relay (browser-to-browser via relay)

---

## Ping-Pong Demo (Original Implementation)

The original callback-based implementation. Still useful as a working reference for libp2p setup.

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
# Use the TCP address from server output (port 9000, without /ws)
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
ping_pong_libp2p/
├── meerkat-net-v2/              # Actor-based network layer (CURRENT)
│   ├── src/
│   │   ├── actor.rs                # NetworkActor + libp2p event loop
│   │   │                           # Composite behaviour: stream + relay + identify
│   │   ├── messages.rs             # NetworkCommand, NetworkReply, NetworkEvent
│   │   ├── mock.rs                 # MockNetwork for testing without libp2p
│   │   ├── network_layer.rs        # NetworkLayer trait
│   │   ├── protocol.rs             # Wire format (length-prefixed JSON)
│   │   ├── types.rs                # MeerkatMessage, Address, NodeType, SendError
│   │   └── lib.rs                  # Module exports
│   ├── tests/
│   │   └── integration_test.rs     # 10 integration tests (real + mock + relay)
│   ├── Cargo.toml
│   └── README.md
│
├── meerkat-net/                 # Original callback-based layer (reference)
│   ├── src/
│   │   ├── interface.rs            # NetworkLayer trait, NetworkCallbacks
│   │   ├── types.rs                # MeerkatMessage, GlobalAddress
│   │   ├── protocol.rs             # Message serialization (length-prefixed)
│   │   ├── libp2p_net.rs           # Real libp2p implementation
│   │   ├── mock.rs                 # Mock for testing
│   │   └── lib.rs                  # Module exports
│   ├── tests/
│   │   ├── basic_test.rs           # Unit tests
│   │   └── libp2p_integration_test.rs  # Integration tests
│   ├── Cargo.toml
│   └── README.md
│
├── shared/                      # Shared protocol types
│   ├── src/
│   │   └── lib.rs                  # PingMessage, PongMessage
│   └── Cargo.toml
│
├── server/                      # Demo libp2p server
│   ├── src/
│   │   └── main.rs                 # TCP + WebSocket server implementation
│   └── Cargo.toml
│
├── client/                      # Demo native client
│   ├── src/
│   │   └── main.rs                 # TCP client implementation
│   └── Cargo.toml
│
├── wasm-client/                 # Demo browser client
│   ├── src/
│   │   └── lib.rs                  # WASM bindings and WebSocket client
│   ├── www/
│   │   ├── index.html              # Browser UI
│   │   └── pkg/                    # WASM build output (generated)
│   ├── Cargo.toml
│   └── README.md
│
├── Cargo.toml                   # Workspace configuration
├── README.md                    # This file
└── .gitignore
```

## Message Protocol

All messages use a length-prefixed format:
```
[4 bytes: length (big-endian u32)][N bytes: JSON payload]
```

Protocol identifier: `/meerkat/1.0.0`

## Implementation Notes

### Circuit Relay v2

The implementation uses libp2p's circuit relay v2 protocol with these key components:

1. **Relay Server Behaviour** - Accepts reservation requests from browser clients
2. **Relay Client Behaviour** - Makes reservations through relay servers  
3. **Identify Protocol** - Exchanges external addresses so relay knows where to route
4. **Automatic Reservation** - Clients automatically request reservation after identify completes

**Critical implementation details:**
- Must use `Multiaddr::with(Protocol::P2pCircuit)` not string concatenation
- Must call `swarm.add_external_address()` when identify receives observed_addr
- Must extract last peer ID from circuit addresses (destination, not relay)
- Must check if address already uses our relay before translating

## Troubleshooting

### Browser Client Won't Connect

**Problem**: Connection fails or shows "MultiaddressNotSupported"

**Solution**: Use the WebSocket address (port 9001 with `/ws`), not the TCP address.

Correct: `/ip4/127.0.0.1/tcp/9001/ws/p2p/12D3KooW...`  
Wrong: `/ip4/127.0.0.1/tcp/9000/p2p/12D3KooW...`

### Circuit Relay NoAddressesInReservation

**Problem**: Relay accepts reservation but client doesn't get circuit address

**Solution**: Relay server must advertise external addresses via identify protocol:
```rust
if let libp2p::identify::Event::Received { info, .. } = event {
    swarm.add_external_address(info.observed_addr.clone());
}
```

### WASM Build Fails

**Solution**:
1. Install wasm-pack: `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`
2. Make sure you're in the `wasm-client` directory
3. Try: `cargo clean` then rebuild

## Dependencies

- `libp2p` 0.56 - Peer-to-peer networking with circuit relay v2
- `libp2p-stream` 0.4.0-alpha - Custom stream protocol
- `kameo` 0.14 - Actor framework
- `tokio` - Async runtime
- `wasm-bindgen` - Rust/JavaScript interop (browser)
- `serde` + `serde_json` - Message serialization

## License

MIT

## Contributing

This project was developed as part of research on distributed reactive systems at SSN College of Engineering under Professor Jonathan Aldrich's guidance.
