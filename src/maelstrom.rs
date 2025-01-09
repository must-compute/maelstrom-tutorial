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
        msg_id: usize,
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
    Echo {
        msg_id: usize,
        echo: serde_json::Value,
    },
    EchoOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
        echo: serde_json::Value,
    },
    Topology {
        msg_id: usize,
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
    Broadcast {
        msg_id: Option<usize>, // TODO is this correct? "Inter-server messages don't have a msg_id" per docs
        message: serde_json::Value,
    },
    BroadcastOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
    Read {
        msg_id: usize,
    },
    ReadOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
        messages: HashSet<serde_json::Value>,
    },
    Todo(serde_json::Value),
}
