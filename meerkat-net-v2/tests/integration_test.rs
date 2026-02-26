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

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_messages() {
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

    let mut client = NetworkActor::new(NodeType::Server).await.unwrap();

    for i in 0..5 {
        client.handle_command(NetworkCommand::SendMessage {
            addr: full_addr.clone(),
            msg: MeerkatMessage::Ping {
                content: format!("Message {}", i),
            },
        }).await;
    }

    let mut received = 0;
    for _ in 0..100 {
        sleep(Duration::from_millis(100)).await;
        while let Ok(event) = server.event_rx.try_recv() {
            if let NetworkEvent::MessageReceived { .. } = event {
                received += 1;
            }
        }
        if received >= 5 { break; }
    }

    assert_eq!(received, 5, "Server should have received all 5 messages, got {}", received);
    println!("✓ Multiple messages test passed!");
}

// ── Mock network tests ────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn test_mock_send_and_receive() {
    let registry = MockNetwork::new_registry();

    let mut server = MockNetwork::new_with_registry(registry.clone());
    let mut client = MockNetwork::new_with_registry(registry.clone());

    // Listen to get a routable address
    let reply = server.handle_command(NetworkCommand::Listen {
        addr: Address::new("/ip4/127.0.0.1/tcp/9000"),
    }).await;

    let server_addr = match reply {
        NetworkReply::ListenSuccess { addr } => addr,
        other => panic!("Expected ListenSuccess, got {:?}", other),
    };

    println!("Mock server address: {}", server_addr.0);

    // Send from client to server
    client.handle_command(NetworkCommand::SendMessage {
        addr: server_addr,
        msg: MeerkatMessage::Ping {
            content: "hello from mock client".to_string(),
        },
    }).await;

    // Message should be delivered instantly — no sleep needed
    let event = server.event_rx.try_recv().expect("Server should have received a message");

    if let NetworkEvent::MessageReceived { msg, .. } = event {
        if let MeerkatMessage::Ping { content } = msg {
            assert_eq!(content, "hello from mock client");
            println!("✓ Mock send and receive test passed!");
        }
    } else {
        panic!("Expected MessageReceived event");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mock_multiple_messages() {
    let registry = MockNetwork::new_registry();
    let mut server = MockNetwork::new_with_registry(registry.clone());
    let mut client = MockNetwork::new_with_registry(registry.clone());

    let reply = server.handle_command(NetworkCommand::Listen {
        addr: Address::new("/ip4/127.0.0.1/tcp/9000"),
    }).await;

    let server_addr = match reply {
        NetworkReply::ListenSuccess { addr } => addr,
        other => panic!("Expected ListenSuccess, got {:?}", other),
    };

    for i in 0..5 {
        client.handle_command(NetworkCommand::SendMessage {
            addr: server_addr.clone(),
            msg: MeerkatMessage::Ping {
                content: format!("Message {}", i),
            },
        }).await;
    }

    let mut received = 0;
    while let Ok(event) = server.event_rx.try_recv() {
        if let NetworkEvent::MessageReceived { .. } = event {
            received += 1;
        }
    }

    assert_eq!(received, 5, "Expected 5 messages, got {}", received);
    println!("✓ Mock multiple messages test passed!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mock_unreachable_address() {
    let mut client = MockNetwork::new();

    client.handle_command(NetworkCommand::SendMessage {
        addr: Address::new("/ip4/127.0.0.1/tcp/9000/p2p/nonexistent-peer"),
        msg: MeerkatMessage::Ping {
            content: "this should fail".to_string(),
        },
    }).await;

    let event = client.event_rx.try_recv().expect("Should have received SendFailed");
    assert!(
        matches!(event, NetworkEvent::SendFailed { .. }),
        "Expected SendFailed, got {:?}", event
    );
    println!("✓ Mock unreachable address test passed!");
}

// ── NetworkLayer trait tests ──────────────────────────────────────────────────

async fn send_ping_via_trait<N: meerkat_net_v2::NetworkLayer>(
    sender: &mut N,
    addr: Address,
) {
    sender.handle_command(NetworkCommand::SendMessage {
        addr,
        msg: MeerkatMessage::Ping {
            content: "via trait".to_string(),
        },
    }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trait_with_mock() {
    let registry = MockNetwork::new_registry();
    let mut server = MockNetwork::new_with_registry(registry.clone());
    let mut client = MockNetwork::new_with_registry(registry.clone());

    let reply = server.handle_command(NetworkCommand::Listen {
        addr: Address::new("/ip4/127.0.0.1/tcp/9000"),
    }).await;

    let server_addr = match reply {
        NetworkReply::ListenSuccess { addr } => addr,
        other => panic!("Expected ListenSuccess, got {:?}", other),
    };

    send_ping_via_trait(&mut client, server_addr).await;

    let event = server.try_recv_event().expect("Should have received event");
    assert!(matches!(event, NetworkEvent::MessageReceived { .. }));
    println!("✓ Trait with mock test passed!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_trait_with_real_network() {
    let mut server = NetworkActor::new(NodeType::Server).await.unwrap();

    let reply = server.handle_command(NetworkCommand::Listen {
        addr: Address::new("/ip4/127.0.0.1/tcp/0"),
    }).await;

    let server_addr = match reply {
        NetworkReply::ListenSuccess { addr } => addr,
        other => panic!("Expected ListenSuccess, got {:?}", other),
    };

    let full_addr = Address::new(format!("{}/p2p/{}", server_addr.0, server.local_peer_id()));

    let mut client = NetworkActor::new(NodeType::Server).await.unwrap();
    send_ping_via_trait(&mut client, full_addr).await;

    let mut received = false;
    for _ in 0..50 {
        sleep(Duration::from_millis(100)).await;
        if let Some(event) = server.try_recv_event() {
            if let NetworkEvent::MessageReceived { .. } = event {
                received = true;
                break;
            }
        }
    }

    assert!(received, "Server never received the ping via trait");
    println!("✓ Trait with real network test passed!");
}
