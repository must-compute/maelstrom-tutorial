use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

use std::collections::HashMap;

use super::micro_op::MicroOperation;

pub type Transaction = Vec<MicroOperation>;

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
    Txn {
        msg_id: usize,
        txn: Vec<MicroOperation>,
    },
    TxnOk {
        txn: Vec<MicroOperation>,
        in_reply_to: usize,
    },
    // lin-kv, lww-kv
    Read {
        msg_id: usize,
        key: String, // technically it should be Any
    },
    ReadOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
        value: serde_json::Value,
    },
    Write {
        msg_id: usize,
        key: String, // technically it should be Any
        value: serde_json::Value,
    },
    WriteOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
    Cas {
        msg_id: usize,
        key: String, // technically it should be Any
        from: serde_json::Value,
        to: serde_json::Value,
        create_if_not_exists: bool, //NOTE: this is only supported by lin-kv, not lww-kv.
    },
    CasOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
    Error {
        in_reply_to: usize,
        code: ErrorCode,
        text: String,
    },
}

impl Body {
    pub fn msg_id(&self) -> usize {
        match self {
            Body::Init { msg_id, .. } => *msg_id,
            Body::InitOk { msg_id, .. } => msg_id.unwrap(),
            Body::Topology { msg_id, .. } => *msg_id,
            Body::TopologyOk { msg_id, .. } => msg_id.unwrap(),
            Body::Txn { msg_id, .. } => *msg_id,
            Body::TxnOk { .. } => unreachable!(),
            Body::Read { msg_id, .. } => *msg_id,
            Body::ReadOk { msg_id, .. } => msg_id.unwrap(),
            Body::Write { msg_id, .. } => *msg_id,
            Body::WriteOk { msg_id, .. } => msg_id.unwrap(),
            Body::Cas { msg_id, .. } => *msg_id,
            Body::CasOk { msg_id, .. } => msg_id.unwrap(),
            Body::Error { .. } => panic!("error msgs have no msg id"), // this inidicates an issue with the body type. TODO cleaner design
        }
    }
    pub fn set_msg_id(&mut self, new_id: usize) {
        match self {
            Body::Init { ref mut msg_id, .. } => *msg_id = new_id,
            Body::InitOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Topology { ref mut msg_id, .. } => *msg_id = new_id,
            Body::TopologyOk { ref mut msg_id, .. } => *msg_id = Some(new_id),
            Body::Txn { ref mut msg_id, .. } => *msg_id = new_id,
            Body::TxnOk { .. } => unreachable!(),
            Body::Read { msg_id, .. } => *msg_id = new_id,
            Body::ReadOk { msg_id, .. } => *msg_id = Some(new_id),
            Body::Write { msg_id, .. } => *msg_id = new_id,
            Body::WriteOk { msg_id, .. } => *msg_id = Some(new_id),
            Body::Cas { msg_id, .. } => *msg_id = new_id,
            Body::CasOk { msg_id, .. } => *msg_id = Some(new_id),
            Body::Error { .. } => panic!("error msgs have no msg id"),
        };
    }
    pub fn in_reply_to(&self) -> usize {
        match self {
            Body::TxnOk { in_reply_to, .. } => *in_reply_to,
            Body::ReadOk { in_reply_to, .. } => *in_reply_to,
            Body::WriteOk { in_reply_to, .. } => *in_reply_to,
            Body::CasOk { in_reply_to, .. } => *in_reply_to,
            Body::Error { in_reply_to, .. } => *in_reply_to,
            _ => panic!("shouldn't be used"),
        }
    }
}

// https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors
#[derive(Serialize_repr, Deserialize_repr, PartialEq, Debug, Clone)]
#[repr(u8)]
pub enum ErrorCode {
    Timeout = 0,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    PreconditionFailed = 22,
    TxnConflict = 30,
}
