use tokio::sync::mpsc::Sender;

use super::{
    log::Log,
    message::Message,
    raft::{NodeState, Raft, StateMachineKey, StateMachineValue},
};

// TODO use Result<T, Error>
type ChannelResponder<T> = tokio::sync::oneshot::Sender<T>;

pub enum Event {
    Call(Query),
    Cast(Command),
}

// For events with a notifier channel for response
pub enum Query {
    ReserveMsgId {
        responder: ChannelResponder<usize>,
    },
    RaftSnapshot {
        responder: ChannelResponder<Raft>,
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
    // BecomeFollowerOf {
    //     leader: String,
    // },
    // SetVotedFor {
    //     candidate: String,
    // },
    UpdateRaftWith {
        updater: fn(&mut Raft) -> (),
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
