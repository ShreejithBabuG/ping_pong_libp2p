# Ping Pong libp2p

A peer-to-peer messaging demonstration using Rust and libp2p, supporting native clients and WebAssembly browsers.

This repository contains two main components:
1. **Ping-Pong Demo** - Working example of libp2p messaging (server, native client, browser client)
2. **Meerkat-Net** - Clean network abstraction layer with callback-based interface

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
           │ Shared Protocol
           ▼
    ┌──────────────────┐
    │  PingMessage     │
    │  PongMessage     │
    │  /meerkat-ping   │
    └──────────────────┘
```

## Features

- **Multi-platform support**: Native Rust clients and browser WASM clients
- **Multiple transports**: TCP for native clients, WebSocket for browsers
- **Shared types**: Same Rust message structs across all platforms
- **Length-prefixed protocol**: 4-byte length + JSON payload
- **No actor framework**: Direct libp2p stream usage

## Meerkat-Net: Network Abstraction Layer

The `meerkat-net` module provides a clean interface for peer-to-peer networking that:

- **Separates concerns**: Hides libp2p transport details from application logic
- **Global addresses**: Uses internet-routable addresses that the net layer converts to libp2p routing
- **Callback-based**: Non-blocking sends with error/message callbacks
- **Type-safe messages**: Enum-based message types
- **Async interface**: Modern async/await pattern

### Design Principles
```rust
// Application defines messages
enum MeerkatMessage {
    Ping { content: String },
    Pong { content: String },
}

// Application provides callbacks
let callbacks = NetworkCallbacks {
    on_message: Arc::new(|peer, msg| { /* handle message */ }),
    on_send_error: Arc::new(|msg_id, error| { /* handle error */ }),
};

// Net layer handles transport
let mut net = LibP2PNetwork::new(callbacks)?;
net.listen(GlobalAddress::new("/ip4/0.0.0.0/tcp/9000")).await?;

// Send is non-blocking, returns immediately
let msg_id = net.send(peer_addr, MeerkatMessage::Ping { ... });
```

### Key Components

- **`NetworkLayer` trait** - Async interface for send/listen/addresses
- **`MeerkatMessage` enum** - Type-safe message definitions
- **`GlobalAddress`** - Internet-routable address format
- **`NetworkCallbacks`** - Notification functions for events
- **`LibP2PNetwork`** - Real libp2p implementation
- **`MockNetwork`** - Testing without network

### Testing
```bash
# Run all meerkat-net tests
cargo test -p meerkat-net

# Run integration tests with real libp2p
cargo test -p meerkat-net --test libp2p_integration_test
```

## Quick Start: Ping-Pong Demo

### Prerequisites

- Rust 1.70 or later
- wasm-pack (for browser client): `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`
- Python 3 (for serving WASM)

### 1. Clone the Repository
```bash
git clone https://github.com/ShreejithBabuG/ping_pong_libp2p.git
cd ping_pong_libp2p
```

### 2. Run the Server

Open a terminal and run:
```bash
cargo run -p meerkat-server
```

You should see output like:
```
Meerkat Server (libp2p)
TCP + WebSocket Support

Server ready
  TCP:       127.0.0.1:9000 (for native clients)
  WebSocket: 127.0.0.1:9001 (for browser clients)

Listening on: /ip4/127.0.0.1/tcp/9001/ws/p2p/12D3KooW...
Listening on: /ip4/127.0.0.1/tcp/9000/p2p/12D3KooW...
```

### 3. Run the Native Client

Open a **new terminal** and run:
```bash
# Use the TCP address from server output (port 9000, without /ws)
cargo run -p meerkat-client -- /ip4/127.0.0.1/tcp/9000/p2p/12D3KooW...
```

Replace `12D3KooW...` with the actual peer ID from your server output.

You should see ping-pong messages every 2 seconds:
```
Meerkat Client (libp2p)

Sending: Hello from Meerkat client (ping #1)
Received: Pong! You said: Hello from Meerkat client (ping #1)
Ping-Pong #1 complete
```

### 4. Run the Browser Client

#### Build WASM
```bash
cd wasm-client
wasm-pack build --target web
cp -r pkg www/
```

#### Serve the Web Page
```bash
cd www
python3 -m http.server 8080
```

#### Open in Browser

1. Open http://localhost:8080 in your web browser
2. Paste the **WebSocket address** (port 9001 with `/ws`) from the server output
3. Click "Connect to Server"
4. Type a message and click "Send Ping"

## Project Structure
```
ping_pong_libp2p/
├── meerkat-net/         # Network abstraction layer
│   ├── src/
│   │   ├── interface.rs    # NetworkLayer trait
│   │   ├── types.rs        # MeerkatMessage, GlobalAddress
│   │   ├── protocol.rs     # Message serialization
│   │   ├── libp2p_net.rs   # Real libp2p implementation
│   │   └── mock.rs         # Mock for testing
│   └── tests/
│       ├── basic_test.rs              # Unit tests
│       └── libp2p_integration_test.rs # Integration tests
├── shared/              # Ping-pong protocol
│   └── src/lib.rs      # PingMessage, PongMessage
├── server/              # Demo server
│   └── src/main.rs     # TCP + WebSocket server
├── client/              # Demo native client
│   └── src/main.rs     # TCP client
└── wasm-client/         # Demo browser client
    ├── src/lib.rs      # WASM bindings
    └── www/index.html  # Browser UI
```

## Message Protocol

All messages use a length-prefixed format:
```
[4 bytes: length (big-endian u32)][N bytes: JSON payload]
```

Message types:
- `PingMessage { message: String }`
- `PongMessage { message: String }`

Protocol identifier: `/meerkat-ping/1.0.0`

## Troubleshooting

### Browser Client Won't Connect

**Problem**: Connection fails or shows "MultiaddressNotSupported"

**Solution**: Make sure you're using the **WebSocket address** (port 9001 with `/ws`), not the TCP address.

Correct: `/ip4/127.0.0.1/tcp/9001/ws/p2p/12D3KooW...`  
Wrong: `/ip4/127.0.0.1/tcp/9000/p2p/12D3KooW...`

### Native Client Can't Connect

**Problem**: Connection timeout or refused

**Solution**: 
1. Make sure the server is running
2. Use the TCP address (port 9000, **without** `/ws`)
3. Copy the full address including the peer ID

### WASM Build Fails

**Problem**: `wasm-pack` errors

**Solution**:
1. Install wasm-pack: `curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh`
2. Make sure you're in the `wasm-client` directory
3. Try: `cargo clean` then rebuild

## Dependencies

Key libraries used:

- `libp2p` 0.56 - Peer-to-peer networking
- `libp2p-stream` 0.4.0-alpha - Custom stream protocol
- `tokio` - Async runtime (native)
- `async-trait` - Async trait methods
- `wasm-bindgen` - Rust/JavaScript interop (browser)
- `serde` + `serde_json` - Message serialization

## License

MIT

## Contributing

Contributions welcome! Please open an issue or submit a pull request.
