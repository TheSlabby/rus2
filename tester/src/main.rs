//! Dumbest possible downstream user of `rus2`.
//!
//! Run it in two terminals and they should find each other:
//!
//!     cargo run -p tester -- alice
//!     cargo run -p tester -- bob
//!
//! It only needs this much of the library to exist:
//!
//!     Node::builder(&str) -> NodeBuilder
//!     NodeBuilder::spawn(self) -> impl Future<Output = Result<Node, E>>
//!     Node::peers(&self) -> Vec<Peer>          // Peer: Debug
//!
//! where E: std::error::Error + Send + Sync + 'static.

use std::time::Duration;

use rus2::Node;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let name = std::env::args().nth(1).unwrap_or_else(|| "tester".to_string());

    let node = Node::builder(&name).spawn().await?;
    println!("[{name}] up — watching for peers, ctrl-c to quit");

    let mut last = String::new();
    loop {
        // Only print when the peer set actually changes, so the terminal
        // stays readable while you're staring at it.
        let peers = node.peers();
        let now = format!("{peers:?}");
        if now != last {
            println!("[{name}] peers ({}): {now}", peers.len());
            last = now;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
