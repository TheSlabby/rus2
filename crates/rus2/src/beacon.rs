use std::{io, sync::Arc};
use tokio::net::UdpSocket;
use serde::{Serialize, Deserialize};
use serde_json;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::wire::{BeaconMessage, Packet};

use tokio::time::Duration;
use crate::node::Node;



pub struct Beacon {
    node: Arc<Node>,
    socket: Arc<UdpSocket>
}

impl Beacon {
    pub fn new(n: Arc<Node>, s: Arc<UdpSocket>) -> Beacon {
        Beacon{node: n, socket: s}
    }

    pub async fn run(self) -> io::Result<()> {

        // send loop
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            let mut buf = [0u8; 1024];
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        println!("TICK!");
                        // let packet = BeaconMessage{sender_name: self.node.}
                        let p = BeaconMessage{
                            sender_name: self.node.name().to_string(), // "test".to_string(),
                            epoch_ns: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
                        };
                        let packet = Packet::Beacon(p);

                        // serialize packet
                        let bytes = match serde_json::to_vec(&packet) {
                            Ok(b) => b,
                            Err(e) => { eprintln!("encode: {}", e); continue; }
                        };

                        self.socket.send_to(&bytes, "255.255.255.255:5060").await;
                    }
                    res = self.socket.recv_from(&mut buf) => {
                        match (res) {
                            Ok((n, addr)) => {
                                println!("GOT DATA!");

                                // try to deserialize
                                match serde_json::from_slice::<Packet>(&buf[..n]) {
                                    Ok(Packet::Beacon(msg)) => {
                                        self.node.process_beacon_heartbeat(&msg.sender_name, &addr).await;
                                    },
                                    Ok(Packet::Data(msg)) => {},
                                    Err(e) => { eprintln!("error: {e}"); }
                                }
                            },
                            Err(e) => {
                                eprintln!("error: {e}");
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }
}

pub async fn hello_world() -> io::Result<()> {
    println!("HELLO BR0");

    Ok(())
}
