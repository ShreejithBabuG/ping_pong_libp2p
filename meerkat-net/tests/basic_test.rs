use meerkat_net::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[tokio::test]
async fn test_mock_ping_pong() {
    // Track if message was received
    let received = Arc::new(AtomicBool::new(false));
    let received_clone = received.clone();
    
    // Create shared state
    let shared = Arc::new(std::sync::Mutex::new(
        SharedMockState::default()
    ));
    
    // Create callbacks for net1
    let callbacks1 = NetworkCallbacks {
        on_message: Arc::new(|peer, msg| {
            println!("Net1 received from {}: {:?}", peer, msg);
        }),
        on_send_error: Arc::new(|_id, err| {
            println!("Net1 send error: {:?}", err);
        }),
        on_peer_connected: None,
        on_peer_disconnected: None,
    };
    
    // Create callbacks for net2 - THIS is where we track receipt
    let callbacks2 = NetworkCallbacks {
        on_message: Arc::new(move |peer, msg| {
            println!("Net2 received from {}: {:?}", peer, msg);
            received_clone.store(true, Ordering::SeqCst);
        }),
        on_send_error: Arc::new(|_id, err| {
            println!("Net2 send error: {:?}", err);
        }),
        on_peer_connected: None,
        on_peer_disconnected: None,
    };
    
    let mut net1 = MockNetwork::new(callbacks1, shared.clone());
    let mut net2 = MockNetwork::new(callbacks2, shared.clone());
    
    // Make net2 listen on an address
    let addr2 = GlobalAddress::new("mock://peer2");
    net2.listen(addr2.clone()).await.unwrap();
    
    // Net1 sends a message to net2
    let msg = MeerkatMessage::Ping {
        content: "Hello!".to_string(),
    };
    
    let msg_id = net1.send(addr2, msg);
    println!("Sent message with ID: {:?}", msg_id);
    
    // Give async delivery time to complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Check if received
    assert!(received.load(Ordering::SeqCst), "Message was not received!");
    println!("✓ Test passed!");
}
