use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Message {
    pub src: String,
    pub dest: String,
    pub body: Body,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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

impl Body {
    pub fn msg_id(&self) -> usize {
        match self {
            Body::Init { msg_id, .. } => *msg_id,
            Body::InitOk { msg_id, .. } => msg_id.unwrap(),
            Body::Echo { msg_id, .. } => *msg_id,
            Body::EchoOk { msg_id, .. } => msg_id.unwrap(),
            Body::Topology { msg_id, .. } => *msg_id,
            Body::TopologyOk { msg_id, .. } => msg_id.unwrap(),
            Body::Broadcast { msg_id, .. } => msg_id.unwrap(),
            Body::BroadcastOk { msg_id, .. } => msg_id.unwrap(),
            Body::Read { msg_id } => *msg_id,
            Body::ReadOk { msg_id, .. } => msg_id.unwrap(),
            Body::Todo(_) => todo!(),
        }
    }
    pub fn set_msg_id(&mut self, new_id: usize) {
        match self {
            Body::Init { ref mut msg_id, .. } => *msg_id = new_id,
            Body::InitOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Echo { ref mut msg_id, .. } => *msg_id = new_id,
            Body::EchoOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Topology { ref mut msg_id, .. } => *msg_id = new_id,
            Body::TopologyOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Broadcast { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::BroadcastOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Read { ref mut msg_id } => *msg_id = new_id,
            Body::ReadOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Todo(_) => todo!(),
        };
    }
}
