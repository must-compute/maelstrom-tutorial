use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};

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
    // lin-kv, lww-kv
    Read {
        msg_id: usize,
        key: usize, // technically it should be Any
    },
    ReadOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
        value: serde_json::Value,
    },
    Write {
        msg_id: usize,
        key: usize, // technically it should be Any
        value: serde_json::Value,
    },
    WriteOk {
        msg_id: Option<usize>,
        in_reply_to: usize,
    },
    Cas {
        msg_id: usize,
        key: usize, // technically it should be Any
        from: serde_json::Value,
        to: serde_json::Value,
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
    RequestVote {
        msg_id: usize,
        term: usize,
        candidate_id: String,
        last_log_index: usize,
        last_log_term: usize,
    },
    RequestVoteOk {
        msg_id: usize,
        in_reply_to: usize,
        term: usize,
        vote_granted: bool,
    },
}

impl Body {
    pub fn msg_id(&self) -> usize {
        match self {
            Body::Init { msg_id, .. } => *msg_id,
            Body::InitOk { msg_id, .. } => msg_id.unwrap(),
            Body::Read { msg_id, .. } => *msg_id,
            Body::ReadOk { msg_id, .. } => msg_id.unwrap(),
            Body::Write { msg_id, .. } => *msg_id,
            Body::WriteOk { msg_id, .. } => msg_id.unwrap(),
            Body::Cas { msg_id, .. } => *msg_id,
            Body::CasOk { msg_id, .. } => msg_id.unwrap(),
            Body::Error { .. } => panic!("error msgs have no msg id"),
            Body::RequestVote { msg_id, .. } => *msg_id,
            Body::RequestVoteOk { msg_id, .. } => *msg_id, // this inidicates an issue with the body type. TODO cleaner design
        }
    }
    pub fn in_reply_to(&self) -> usize {
        match self {
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

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::Timeout => write!(f, "timeout"),
            ErrorCode::NotSupported => write!(f, "not supported"),
            ErrorCode::TemporarilyUnavailable => write!(f, "temporarily unavailable"),
            ErrorCode::MalformedRequest => write!(f, "malformed request"),
            ErrorCode::Crash => write!(f, "crash"),
            ErrorCode::Abort => write!(f, "abort"),
            ErrorCode::KeyDoesNotExist => write!(f, "key does not exist"),
            ErrorCode::PreconditionFailed => write!(f, "precondition failed"),
            ErrorCode::TxnConflict => write!(f, "txn conflict"),
        }
    }
}

impl std::error::Error for ErrorCode {}
