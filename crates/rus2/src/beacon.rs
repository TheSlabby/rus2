use std::{io, sync::Arc};
use tokio::net::UdpSocket;

use tokio::time::Duration;
use crate::node::Node;


pub struct Beacon {
    node: Arc<Node>,
    socket: UdpSocket
}

impl Beacon {
    pub async fn new(n: Arc<Node>) -> io::Result<Beacon> {
        // setup udp socket
        let s = UdpSocket::bind("0.0.0.0:5061").await?;
        s.set_broadcast(true);

        Ok(Beacon{node: n, socket: s})
    }

    pub async fn run(self) -> io::Result<()> {

        // loop
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                println!("TICK!");
                self.socket.send_to(b"hello, world\r\n", "127.0.0.1:5060").await;
            }
        });

        Ok(())
    }
}

pub async fn hello_world() -> io::Result<()> {
    println!("HELLO BR0");

    Ok(())
}


pub async fn run() -> io::Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:7446").await?;
    sock.set_broadcast(true)?;
    let n = sock.send_to(b"hello", "255.255.255.255:7447").await?;

    Ok(())
}