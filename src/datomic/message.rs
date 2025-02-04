use serde::{Deserialize, Serialize};

use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
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
    Topology {
        msg_id: usize,
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
}

impl Body {
    fn msg_id(&self) -> usize {
        match self {
            Body::Init { msg_id, .. } => *msg_id,
            Body::InitOk { msg_id, .. } => msg_id.unwrap(),
            Body::Topology { msg_id, .. } => *msg_id,
            Body::TopologyOk { msg_id, .. } => msg_id.unwrap(),
        }
    }
    fn set_msg_id(&mut self, new_id: usize) {
        match self {
            Body::Init { ref mut msg_id, .. } => *msg_id = new_id,
            Body::InitOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Topology { ref mut msg_id, .. } => *msg_id = new_id,
            Body::TopologyOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
        };
    }
}
