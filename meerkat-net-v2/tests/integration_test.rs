use meerkat_net_v2::*;
use tokio::time::{sleep, Duration};

#[tokio::test(flavor = "multi_thread")]
async fn test_send_and_receive() {
    let mut server = NetworkActor::new(NodeType::Server).await.unwrap();

    let reply = server.handle_command(NetworkCommand::Listen {
        addr: Address::new("/ip4/127.0.0.1/tcp/0"),
    }).await;

    let server_addr = match reply {
        NetworkReply::ListenSuccess { addr } => addr,
        other => panic!("Expected ListenSuccess, got {:?}", other),
    };

    let server_peer_id = server.local_peer_id();
    let full_addr = Address::new(format!("{}/p2p/{}", server_addr.0, server_peer_id));
    println!("Server full address: {}", full_addr.0);

    let mut client = NetworkActor::new(NodeType::Server).await.unwrap();

    let send_reply = client.handle_command(NetworkCommand::SendMessage {
        addr: full_addr,
        msg: MeerkatMessage::Ping {
            content: "hello from client".to_string(),
        },
    }).await;

    println!("Send reply: {:?}", send_reply);

    let mut received = false;
    for _ in 0..50 {
        sleep(Duration::from_millis(100)).await;
        if let Ok(event) = server.event_rx.try_recv() {
            println!("Server got event: {:?}", event);
            if let NetworkEvent::MessageReceived { msg, .. } = event {
                if let MeerkatMessage::Ping { content } = msg {
                    assert_eq!(content, "hello from client");
                    received = true;
                    break;
                }
            }
        }
    }

    assert!(received, "Server never received the ping");
    println!("✓ Server-to-server test passed!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_translate_address_server() {
    let server = NetworkActor::new(NodeType::Server).await.unwrap();
    // Server should use canonical address directly - no translation
    let canonical = Address::new("/ip4/203.0.113.10/tcp/9000/p2p/12D3KooWXXX/p2p-circuit/p2p/12D3KooWYYY");
    let translated = server.translate_address_pub(&canonical);
    assert_eq!(translated.0, canonical.0);
    println!("✓ Server address translation test passed!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_translate_address_browser_client() {
    let relay = Address::new("/ip4/server1-ip/tcp/9001/ws/p2p/12D3KooWSERVER1");
    let client = NetworkActor::new(NodeType::BrowserClient {
        relay_server: relay.clone(),
    }).await.unwrap();

    let canonical = Address::new("/ip4/203.0.113.10/tcp/9000/p2p/12D3KooWSERVER2/p2p-circuit/p2p/12D3KooWCLIENT2");
    let translated = client.translate_address_pub(&canonical);

    let expected = format!("{}/p2p-circuit/{}", relay.0, canonical.0);
    assert_eq!(translated.0, expected);
    println!("✓ Browser client address translation test passed!");
}
