use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct BeaconMessage {
    pub sender_name: String,
    pub epoch_ns: u64
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataMessage {
    pub topic: String,
    pub payload: Vec<u8>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Packet {
    Beacon(BeaconMessage),
    Data(DataMessage)
}
