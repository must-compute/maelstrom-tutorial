use super::{
    message::Message,
    raft::{StateMachineKey, StateMachineValue},
};

// TODO use Result<T, Error>
type ChannelResponder<T> = tokio::sync::oneshot::Sender<T>;

pub enum Event {
    Call(Query),
    Cast(Command),
}

// For events with a notifier channel for response
pub enum Query {
    GetNodeId {
        responder: ChannelResponder<String>,
    },
    ReserveMsgId {
        responder: ChannelResponder<usize>,
    },
    SendViaMaelstrom {
        message: Message,
        responder: ChannelResponder<Message>,
    },
    KVRead {
        key: StateMachineKey,
        responder: ChannelResponder<Option<StateMachineValue>>,
    },
    KVCas {
        key: StateMachineKey,
        from: StateMachineValue,
        to: StateMachineValue,
        responder: ChannelResponder<anyhow::Result<()>>,
    },
}

// for events with no expected response
pub enum Command {
    Init {
        id: String,
        node_ids: Vec<String>,
    },
    ReceivedViaMaelstrom {
        response: Message,
    },
    SendViaMaelstrom {
        message: Message,
    },
    KVWrite {
        key: StateMachineKey,
        value: StateMachineValue,
    },
}
