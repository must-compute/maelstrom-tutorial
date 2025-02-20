use super::message::Message;

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
        key: String,
        responder: ChannelResponder<Option<String>>,
    },
    KVCas {
        key: String,
        from: String,
        to: String,
        responder: ChannelResponder<anyhow::Result<String>>,
    },
}

// for events with no expected response
pub enum Command {
    Init { id: String, node_ids: Vec<String> },
    ReceivedViaMaelstrom { response: Message },
    SendViaMaelstrom { message: Message },
    KVWrite { key: usize, value: usize },
}
