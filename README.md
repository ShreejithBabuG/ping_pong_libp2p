# Ping Pong libp2p

A peer-to-peer messaging demonstration using Rust and libp2p, supporting native clients and WebAssembly browsers.

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
│   TCP + WebSocket   │  QUIC  │     (Rust)          │
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
- **Multiple transports**: QUIC for native clients, WebSocket for browsers
- **Shared types**: Same Rust message structs across all platforms
- **Length-prefixed protocol**: 4-byte length + JSON payload
- **No actor framework**: Direct libp2p stream usage

## Quick Start

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

**Important**: Copy the WebSocket address (the one with `/ws` - port 9001) for the browser client.

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

You should see the ping-pong exchange in the browser log!

## Project Structure
```
ping_pong_libp2p/
├── shared/              # Shared protocol and message types
│   └── src/lib.rs      # PingMessage, PongMessage, PING_PROTOCOL
├── server/              # Native Rust server
│   └── src/main.rs     # TCP + WebSocket server
├── client/              # Native Rust client
│   └── src/main.rs     # QUIC client
└── wasm-client/         # Browser WebAssembly client
    ├── src/lib.rs      # WASM bindings
    └── www/
        └── index.html  # Browser UI
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

### Server Shows "Address Already in Use"

**Problem**: Ports 9000 or 9001 are already taken

**Solution**: Kill any existing server process or change the ports in `server/src/main.rs`

## How It Works

### Connection Flow

1. **Server starts** and listens on two ports:
   - Port 9000 (TCP) for native Rust clients
   - Port 9001 (WebSocket) for browser clients

2. **Client connects** using the appropriate transport:
   - Native client uses QUIC or TCP
   - Browser client uses WebSocket

3. **Message exchange**:
   - Client opens a stream to server
   - Client sends length-prefixed `PingMessage`
   - Server reads message, creates `PongMessage`
   - Server sends length-prefixed response
   - Client receives and displays pong

### Transport Layer

The same Rust code works across platforms because libp2p abstracts the transport:

- **Native**: Can use QUIC, TCP, or any supported transport
- **Browser**: Limited to WebSocket due to browser security restrictions
- **Server**: Supports multiple transports simultaneously

## Dependencies

Key libraries used:

- `libp2p` 0.56 - Peer-to-peer networking
- `libp2p-stream` 0.4.0-alpha - Custom stream protocol
- `tokio` - Async runtime (native)
- `wasm-bindgen` - Rust/JavaScript interop (browser)
- `serde` + `serde_json` - Message serialization

## Development

### Build All Components
```bash
cargo build --workspace
```

### Run Tests
```bash
cargo test --workspace
```

### Clean Build Artifacts
```bash
cargo clean
rm -rf wasm-client/pkg wasm-client/www/pkg
```

## License

MIT

## Contributing

Contributions welcome! Please open an issue or submit a pull request.
