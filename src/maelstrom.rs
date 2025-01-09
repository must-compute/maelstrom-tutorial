use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Body {
    Init {
        msg_id: u8,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        msg_id: Option<u8>,
        in_reply_to: u8,
    },
    Echo {
        msg_id: u8,
        echo: serde_json::Value,
    },
    EchoOk {
        msg_id: Option<u8>,
        in_reply_to: u8,
        echo: serde_json::Value,
    },
    Topology {
        msg_id: u8,
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {
        msg_id: Option<u8>,
        in_reply_to: u8,
    },
    Broadcast {
        msg_id: Option<u8>, // TODO is this correct? "Inter-server messages don't have a msg_id" per docs
        message: serde_json::Value,
    },
    BroadcastOk {
        msg_id: Option<u8>,
        in_reply_to: u8,
    },
    Read {
        msg_id: u8,
    },
    ReadOk {
        msg_id: Option<u8>,
        in_reply_to: u8,
        messages: HashSet<serde_json::Value>,
    },
    Todo(serde_json::Value),
}
