use meerkat_net::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread")]
async fn test_libp2p_ping_pong() {
    let server_received = Arc::new(AtomicBool::new(false));
    let server_msg_count = Arc::new(AtomicU32::new(0));
    
    let server_received_clone = server_received.clone();
    let server_msg_count_clone = server_msg_count.clone();
    
    let server_callbacks = NetworkCallbacks {
        on_message: Arc::new(move |peer, msg| {
            println!("Server received from {}: {:?}", peer, msg);
            server_received_clone.store(true, Ordering::SeqCst);
            server_msg_count_clone.fetch_add(1, Ordering::SeqCst);
        }),
        on_send_error: Arc::new(|id, err| {
            println!("Server send error {:?}: {}", id, err);
        }),
        on_peer_connected: Some(Arc::new(|peer| {
            println!("Server: Peer connected: {}", peer);
        })),
        on_peer_disconnected: None,
    };
    
    let client_callbacks = NetworkCallbacks {
        on_message: Arc::new(|peer, msg| {
            println!("Client received from {}: {:?}", peer, msg);
        }),
        on_send_error: Arc::new(|id, err| {
            println!("Client send error {:?}: {}", id, err);
        }),
        on_peer_connected: Some(Arc::new(|peer| {
            println!("Client: Peer connected: {}", peer);
        })),
        on_peer_disconnected: None,
    };
    
    let mut server = LibP2PNetwork::new(server_callbacks).unwrap();
    
    let server_addr = GlobalAddress::new("/ip4/127.0.0.1/tcp/0");
    server.listen(server_addr).await.unwrap();
    
    sleep(Duration::from_millis(100)).await;
    
    let server_addrs = server.local_addresses().await;
    assert!(!server_addrs.is_empty(), "Server should have at least one address");
    
    println!("Server listening on: {:?}", server_addrs);
    
    let server_peer_id = server.local_peer_id();
    let server_full_addr = format!("{}/p2p/{}", server_addrs[0].0, server_peer_id);
    let server_global_addr = GlobalAddress::new(server_full_addr);
    
    println!("Server full address: {}", server_global_addr.0);
    
    let mut client = LibP2PNetwork::new(client_callbacks).unwrap();
    
    let ping_msg = MeerkatMessage::Ping {
        content: "Hello from client!".to_string(),
    };
    
    let msg_id = client.send(server_global_addr, ping_msg);
    println!("Client sent message with ID: {:?}", msg_id);
    
    let mut attempts = 0;
    while !server_received.load(Ordering::SeqCst) && attempts < 50 {
        sleep(Duration::from_millis(100)).await;
        attempts += 1;
    }
    
    assert!(
        server_received.load(Ordering::SeqCst),
        "Server should have received the ping message"
    );
    
    assert_eq!(
        server_msg_count.load(Ordering::SeqCst),
        1,
        "Server should have received exactly 1 message"
    );
    
    println!("✓ Integration test passed!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_libp2p_multiple_messages() {
    let msg_count = Arc::new(AtomicU32::new(0));
    let msg_count_clone = msg_count.clone();
    
    let server_callbacks = NetworkCallbacks {
        on_message: Arc::new(move |peer, msg| {
            println!("Server received from {}: {:?}", peer, msg);
            msg_count_clone.fetch_add(1, Ordering::SeqCst);
        }),
        on_send_error: Arc::new(|id, err| {
            println!("Server send error {:?}: {}", id, err);
        }),
        on_peer_connected: None,
        on_peer_disconnected: None,
    };
    
    let client_callbacks = NetworkCallbacks {
        on_message: Arc::new(|peer, msg| {
            println!("Client received from {}: {:?}", peer, msg);
        }),
        on_send_error: Arc::new(|id, err| {
            println!("Client send error {:?}: {}", id, err);
        }),
        on_peer_connected: None,
        on_peer_disconnected: None,
    };
    
    let mut server = LibP2PNetwork::new(server_callbacks).unwrap();
    server.listen(GlobalAddress::new("/ip4/127.0.0.1/tcp/0")).await.unwrap();
    
    sleep(Duration::from_millis(100)).await;
    
    let server_addrs = server.local_addresses().await;
    let server_peer_id = server.local_peer_id();
    let server_addr = GlobalAddress::new(format!("{}/p2p/{}", server_addrs[0].0, server_peer_id));
    
    let mut client = LibP2PNetwork::new(client_callbacks).unwrap();
    
    for i in 0..5 {
        let msg = MeerkatMessage::Ping {
            content: format!("Message {}", i),
        };
        client.send(server_addr.clone(), msg);
    }
    
    let mut attempts = 0;
    while msg_count.load(Ordering::SeqCst) < 5 && attempts < 100 {
        sleep(Duration::from_millis(100)).await;
        attempts += 1;
    }
    
    let received = msg_count.load(Ordering::SeqCst);
    assert_eq!(received, 5, "Server should have received all 5 messages, got {}", received);
    
    println!("✓ Multiple messages test passed!");
}
