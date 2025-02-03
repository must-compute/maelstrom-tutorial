use serde::{Deserialize, Serialize};

use std::collections::{HashMap, HashSet};

pub trait MessageBody {
    fn msg_id(&self) -> usize;
    fn set_msg_id(&mut self, new_id: usize);
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Message<T: MessageBody> {
    pub src: String,
    pub dest: String,
    pub body: Body<T>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum Body<T: MessageBody> {
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
    // used by Broadcast, G-Set, G-Counter
    Read {
        msg_id: usize,
    },
    // TODO rename to BroadcastReadOK
    ReadOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
        messages: HashSet<serde_json::Value>,
    },
    // TODO renamed this due to name clash with GCounterAdd.
    //      So GSet workloads are broken until I fix this.
    //      (it should also be called 'add')
    #[serde(rename = "add2")]
    GSetAdd {
        msg_id: usize,
        element: serde_json::Value,
    },
    #[serde(rename = "add_ok")]
    GSetAddOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
    #[serde(rename = "read_ok")]
    GSetReadOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
        value: HashSet<serde_json::Value>,
    },
    #[serde(rename = "add")]
    GCounterAdd {
        msg_id: usize,
        delta: usize, // TODO or i32?
    },
    #[serde(rename = "add_ok")]
    GCounterAddOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
    #[serde(rename = "read_ok")]
    GCounterReadOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
        value: usize,
    },
    #[serde(untagged)]
    Custom(T),
}

impl<T: MessageBody> MessageBody for Body<T> {
    fn msg_id(&self) -> usize {
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
            Body::GSetAdd { msg_id, .. } => *msg_id,
            Body::GSetAddOk { msg_id, .. } => msg_id.unwrap(),
            Body::GSetReadOk { msg_id, .. } => msg_id.unwrap(),
            Body::GCounterAdd { msg_id, .. } => *msg_id,
            Body::GCounterAddOk { msg_id, .. } => msg_id.unwrap(),
            Body::GCounterReadOk { msg_id, .. } => msg_id.unwrap(),
            Body::Custom(msg) => msg.msg_id(),
        }
    }
    fn set_msg_id(&mut self, new_id: usize) {
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
            Body::GSetAdd { ref mut msg_id, .. } => *msg_id = new_id,
            Body::GSetAddOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::GSetReadOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::GCounterAdd { ref mut msg_id, .. } => *msg_id = new_id,
            Body::GCounterAddOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::GCounterReadOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Custom(msg) => msg.set_msg_id(new_id),
        };
    }
}
