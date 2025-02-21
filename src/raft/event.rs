use tokio::sync::mpsc::Sender;

use super::{
    message::Message,
    raft::{NodeState, StateMachineKey, StateMachineValue, Term},
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
    NodeState {
        responder: ChannelResponder<NodeState>,
    },
    CurrentTerm {
        responder: ChannelResponder<Term>,
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
    SetNodeState(NodeState),
    AdvanceTermTo {
        new_term: usize,
    },
}

pub async fn query<R>(
    event_tx: Sender<Event>,
    build_query: impl FnOnce(ChannelResponder<R>) -> Query,
) -> R {
    let (tx, rx) = tokio::sync::oneshot::channel();
    event_tx
        .send(Event::Call(build_query(tx)))
        .await
        .expect("should be able to send query event");
    rx.await
        .expect("should be able to receive query event response")
}
