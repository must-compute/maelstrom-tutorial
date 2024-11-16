use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "init_ok")]
pub struct InitBody {
    pub msg_id: u8,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "init_ok_body")]
pub struct InitOkBody {
    pub msg_id: Option<u8>,
    pub in_reply_to: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "echo_body")]
pub struct EchoBody {
    pub msg_id: u8,
    pub echo: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "echo_ok_body")]
pub struct EchoOkBody {
    pub msg_id: Option<u8>,
    pub in_reply_to: u8,
    pub echo: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "topology_body")]
pub struct TopologyBody {
    pub msg_id: u8,
    pub topology: HashMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "topology_ok_body")]
pub struct TopologyOkBody {
    pub msg_id: Option<u8>,
    pub in_reply_to: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "broadcast_body")]
pub struct BroadcastBody {
    pub msg_id: Option<u8>, // TODO is this correct? "Inter-server messages don't have a msg_id" per docs
    pub message: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "broadcast_ok_body")]
pub struct BroadcastOkBody {
    pub msg_id: Option<u8>,
    pub in_reply_to: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "read_body")]
pub struct ReadBody {
    pub msg_id: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename = "read_ok_body")]
pub struct ReadOkBody {
    pub msg_id: Option<u8>,
    pub in_reply_to: u8,
    pub messages: HashSet<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Body {
    Init(InitBody),
    InitOk(InitOkBody),
    Echo(EchoBody),
    EchoOk(EchoOkBody),
    Topology(TopologyBody),
    TopologyOk(TopologyOkBody),
    Broadcast(BroadcastBody),
    BroadcastOk(BroadcastOkBody),
    Read(ReadBody),
    ReadOk(ReadOkBody),
    Todo(serde_json::Value),
}
