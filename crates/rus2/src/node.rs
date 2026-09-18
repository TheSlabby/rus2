use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;
use crate::beacon;

pub struct Peer {
    pub name: String,
    pub addr: SocketAddr,
    pub last_seen: Instant
}

pub struct Node {
    name: String,
    peers: Mutex<Vec<Peer>>
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

    
}

impl NodeBuilder {
    pub async fn spawn(self) -> io::Result<Arc<Node>> {
        let n = Arc::new(Node{
            name: self.name.to_string(),
            peers: Mutex::new(Vec::new())
        });

        // create our beacon
        let beacon_node = Arc::clone(&n);
        let b = beacon::Beacon::new(beacon_node).await?;
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