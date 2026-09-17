use std::io;
use std::sync::Arc;
use tokio::time::Duration;
use crate::beacon;

pub struct Node {
    name: String,
}

pub struct NodeBuilder {
    name: String
}


impl Node {
    pub fn builder(name: &str) -> NodeBuilder {
        let builder = NodeBuilder{name: name.to_string()};
        builder
    }
}

impl NodeBuilder {
    pub async fn spawn(self) -> io::Result<Arc<Node>> {
        let n = Arc::new(Node{name: self.name.to_string()});

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