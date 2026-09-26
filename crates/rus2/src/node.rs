use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::net::UdpSocket;
use tokio::time::Instant;
use crate::wire::{DataMessage, Packet};
use crate::beacon;

pub struct Peer {
    pub name: String,
    pub addr: SocketAddr,
    pub last_seen: Instant
}

pub struct Node {
    name: String,
    peers: Mutex<Vec<Peer>>,
    socket: Arc<UdpSocket>,
    callbacks: Mutex<Vec<(String, Arc<dyn Fn(&[u8]) + Send + Sync + 'static>)>>
}

pub struct NodeBuilder {
    name: String
}


impl Node {
    pub fn builder(name: &str) -> NodeBuilder {
        let builder = NodeBuilder{name: name.to_string()};
        builder
    }

    // getters
    pub fn name(&self) -> &str { &self.name }


    pub async fn process_beacon_heartbeat(&self, name: &str, addr: &SocketAddr) -> io::Result<()> {
        println!("processing this heartbeat from {} with name: {}", addr.to_string(), name);
        let mut found = false;
        let mut peers_locked = self.peers.lock().await;
        for peer in peers_locked.iter_mut() {
            if peer.name == name && peer.addr == *addr {
                found = true;
                peer.last_seen = Instant::now();
                break;
            }
        }

        if !found {
            let peer = Peer {
                name: name.to_string(),
                addr: *addr,
                last_seen: Instant::now()
            };
            peers_locked.push(peer);
        }

        Ok(())
    }

    pub async fn process_data_message(&self, data: &DataMessage) -> io::Result<()> {
        println!("GOT DATA MESSAGE on topic: {}", data.topic);

        let callbacks = self.callbacks.lock().await;
        for callback in callbacks.iter() {
            if callback.0 == data.topic {
                callback.1(&data.payload);
            }
        }


        Ok(())
    }

    
    // TODO: make topics support multiple types (not just T)
    pub async fn publish<T: Serialize>(&self, topic: &str, data: &T) -> io::Result<()> {
        let s: &Arc<UdpSocket> = &self.socket;
        let payload_bytes = serde_json::to_vec(data)?;
        // wrap in packet
        let packet = Packet::Data(DataMessage{topic: topic.to_string(), payload: payload_bytes});
        let bytes = serde_json::to_vec(&packet)?;
        for peer in self.peers.lock().await.iter() {
            s.send_to(&bytes, peer.addr).await?;
        }

        Ok(())
    }

    pub async fn add_callback(&self, topic: &str, callback: Arc<dyn Fn(&[u8]) + Sync + Send + 'static>) -> io::Result<()> {
        let mut callbacks = self.callbacks.lock().await;
        callbacks.push((topic.to_string(), callback));

        Ok(())
    }

    pub async fn subscribe<T: DeserializeOwned + 'static>(&self, topic: &str, handler: Arc<dyn Fn(T) + Sync + Send + 'static>) -> io::Result<()> {
        // callback that tries to parse in T format
        self.add_callback(topic, Arc::new(move |message| {
            match serde_json::from_slice::<T>(message) {
                Ok(value) => {
                    handler(value)
                },
                Err(e) => eprintln!("Error: {e}")
            }
        })).await?;

        Ok(())
    }

    
}

impl NodeBuilder {
    pub async fn spawn(self) -> io::Result<Arc<Node>> {
        // create socket for the node & the beacon
        let socket = Arc::new(UdpSocket::bind("0.0.0.0:5060").await?);
        socket.set_broadcast(true)?;

        let n = Arc::new(Node{
            name: self.name.to_string(),
            peers: Mutex::new(Vec::new()),
            socket: Arc::clone(&socket),
            callbacks: Mutex::new(Vec::new())
        });

        // create our beacon
        let beacon_node = Arc::clone(&n);
        let b = beacon::Beacon::new(beacon_node, Arc::clone(&socket));
        b.run().await?;

        // tokio timers etc
        // let node_in_thread = Arc::clone(&n);
        // tokio::spawn(async move {
        //     let mut ticker = tokio::time::interval(Duration::from_secs(1));
        //     loop {
        //         ticker.tick().await;
        //         println!("tick");
        //         println!("node in thread: {}", node_in_thread.name);
        //     }
        // });
        

        Ok(n)
    }
}