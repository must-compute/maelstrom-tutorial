use std::{collections::HashMap, time::Duration};

use rand::Rng;
use tokio::{select, time::Interval};

use super::{
    event::{query, Command, Event, Query},
    kv_store::KeyValueStore,
    log::Log,
    message::{Body, ErrorCode, Message},
};
use futures::stream::{select_all, FuturesUnordered, StreamExt};

pub type StateMachineKey = usize;
pub type StateMachineValue = usize;

type LeaderId = String;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Leader,
    Candidate,
    FollowerOf(LeaderId),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Term(usize);

impl Term {
    pub fn new() -> Self {
        Term(0)
    }

    pub fn advance_to(&mut self, new_term: usize) {
        assert!(new_term > self.0);
        self.0 = new_term;
    }

    pub fn get(&self) -> usize {
        self.0
    }
}

pub async fn run() {
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Event>(32);

    // event handler task
    tokio::spawn(async move {
        let mut unacked: HashMap<usize, tokio::sync::oneshot::Sender<Message>> = Default::default();
        let mut node_id: String = Default::default();
        let mut other_node_ids: Vec<String> = Default::default();
        let mut next_msg_id = 0;
        let mut state_machine: KeyValueStore<StateMachineKey, StateMachineValue> =
            Default::default();
        let mut node_state = NodeState::FollowerOf("TODO DETERMINE A SANE DEFAULT".to_string());
        let mut term = Term::new();
        let mut log = Log::new();

        while let Some(event) = event_rx.recv().await {
            match event {
                Event::Cast(Command::Init { id, node_ids }) => {
                    node_id = id;
                    other_node_ids = node_ids.into_iter().filter(|id| *id != node_id).collect();
                }
                Event::Call(Query::GetNodeId { responder }) => responder
                    .send(node_id.clone())
                    .expect("respond to Query::GetNodeId"),
                Event::Call(Query::SendViaMaelstrom { ref message, .. })
                | Event::Cast(Command::SendViaMaelstrom { ref message }) => {
                    let message = message.clone();
                    println!(
                        "{}",
                        serde_json::to_string(&message)
                            .expect("msg being sent to STDOUT should be serializable to JSON")
                    );

                    tracing::debug!("sent msg {:?}", &message);

                    match event {
                        Event::Call(Query::SendViaMaelstrom { responder, .. }) => {
                            // TODO i dont like this at all, because     ^message
                            //      will have an old msg id if captured, due to shadowing
                            let msg_id = message.body.msg_id();
                            unacked.insert(msg_id, responder);
                        }
                        _ => (),
                    };
                }
                Event::Cast(Command::ReceivedViaMaelstrom { response }) => {
                    // we received an ack, so we notify and remove from unacked.
                    if let Some(notifier) = unacked.remove(&response.body.in_reply_to()) {
                        notifier
                            .send(response)
                            .expect("returning msg ack should work over the oneshot channel");
                    }
                }
                Event::Call(Query::ReserveMsgId { responder: tx }) => {
                    tx.send(next_msg_id).unwrap();
                    next_msg_id += 1;
                }
                Event::Call(Query::KVRead { key, responder }) => {
                    responder
                        .send(state_machine.read(&key).map(|v| v.to_owned()))
                        .expect("should be able to respond to KVRead over oneshot channel");
                }
                Event::Cast(Command::KVWrite { key, value }) => {
                    state_machine.write(key, value);
                }
                Event::Call(Query::KVCas {
                    key,
                    from,
                    to,
                    responder,
                }) => {
                    responder
                        .send(state_machine.cas(key, from, to))
                        .expect("should be able to respond to KVCas over oneshot channel");
                }
                Event::Call(Query::NodeState { responder }) => responder
                    .send(node_state.clone())
                    .expect("event handler should be able to send node_state"),
                Event::Cast(Command::SetNodeState(new_state)) => node_state = new_state,
                Event::Call(Query::CurrentTerm { responder }) => responder
                    .send(term.clone())
                    .expect("event handler should be abel to send current term"),
                Event::Cast(Command::AdvanceTermTo { new_term }) => term.advance_to(new_term),
                Event::Call(Query::GetOtherNodeIds { responder }) => {
                    responder
                        .send(other_node_ids.clone())
                        .expect("should be able to send other node ids back");
                }
            }
        }
    });

    // stdin task
    tokio::spawn(async move {
        let mut input = String::new();
        let mut is_reading_stdin = true;
        while is_reading_stdin {
            if let Err(e) = std::io::stdin().read_line(&mut input) {
                println!("readline error: {e}");
                is_reading_stdin = false;
            }

            let json_msg = serde_json::from_str(&input)
                .expect(&format!("should take a JSON message. Got {:?}", input));
            tracing::debug!("received json msg: {:?}", json_msg);
            stdin_tx.send(json_msg).await.unwrap();
            input.clear();
        }
    });

    let mut rng = rand::rng();
    let start = tokio::time::Instant::now() + Duration::from_millis(rng.random_range(1000..=3000));
    let mut election_deadline = tokio::time::interval_at(start, Duration::from_millis(1000));
    loop {
        tokio::select! {
            Some(json_msg) = stdin_rx.recv() => {
                tokio::spawn({
                    let event_tx = event_tx.clone();
                    async move {handle(event_tx, json_msg).await}
                });
            }
            _ = election_deadline.tick() => { election_deadline = handle_election_tick(event_tx.clone()).await; }
        }
    }
}

async fn handle_election_tick(event_tx: tokio::sync::mpsc::Sender<Event>) -> Interval {
    let node_state = query(event_tx.clone(), |responder| Query::NodeState { responder }).await;
    match node_state {
        NodeState::Candidate | NodeState::FollowerOf(_) => {
            event_tx
                .send(Event::Cast(Command::SetNodeState(NodeState::Candidate)))
                .await
                .expect("should be able to send BecomeCandidate event");
            let term = query(event_tx.clone(), |responder| Query::CurrentTerm {
                responder,
            })
            .await;
            let new_term = term.get() + 1;
            event_tx
                .send(Event::Cast(Command::AdvanceTermTo { new_term }))
                .await
                .expect("should be able to send AdvanceTermTo event");
            eprintln!("became a candidate for {new_term}");

            let (tx, rx) = tokio::sync::mpsc::channel::<Message>(1000);
            broadcast(
                event_tx.clone(),
                Body::RequestVote {
                    msg_id: todo!(),
                    term: todo!(),
                    candidate_id: todo!(),
                    last_log_index: todo!(),
                    last_log_term: todo!(),
                },
                Some(tx),
            )
            .await;

            while let Some(f) = rx.recv().await {
                todo!();
            }
        }
        NodeState::Leader => {}
    };
    let mut rng = rand::rng();
    let new_duration = Duration::from_millis(rng.random_range(1000..=3000));
    let new_election_deadline =
        tokio::time::interval_at(tokio::time::Instant::now() + new_duration, new_duration);
    eprintln!(
        "Timer elapsed, next interval: {}ms",
        new_duration.as_millis()
    );
    new_election_deadline
}

async fn handle(event_tx: tokio::sync::mpsc::Sender<Event>, msg: Message) -> () {
    match msg.body {
        Body::Init {
            msg_id,
            node_id,
            node_ids,
        } => {
            event_tx
                .send(Event::Cast(Command::Init {
                    id: node_id,
                    node_ids,
                }))
                .await
                .expect("should be able to send Init event");

            send(
                event_tx.clone(),
                None,
                msg.src,
                Body::InitOk {
                    msg_id: Some(
                        query(event_tx, |responder| Query::ReserveMsgId { responder }).await,
                    ),
                    in_reply_to: msg_id,
                },
            )
            .await;
        }
        Body::Read { msg_id, key } => {
            let (tx, rx) = tokio::sync::oneshot::channel::<Option<StateMachineValue>>();
            event_tx
                .send(Event::Call(Query::KVRead { key, responder: tx }))
                .await
                .expect("should be able to send Read event");
            let response = rx
                .await
                .expect("should be able to read from Read oneshot channel");

            send(
                event_tx.clone(),
                None,
                msg.src,
                match response {
                    Some(value) => Body::ReadOk {
                        msg_id: Some(
                            query(event_tx, |responder| Query::ReserveMsgId { responder }).await,
                        ),
                        in_reply_to: msg_id,
                        value: serde_json::to_value(&value)
                            .expect("value should be serializable to json"),
                    },
                    None => {
                        let err = ErrorCode::KeyDoesNotExist;
                        Body::Error {
                            in_reply_to: msg_id,
                            code: err.clone(),
                            text: err.to_string(),
                        }
                    }
                },
            )
            .await
        }
        Body::Write { msg_id, key, value } => {
            let value =
                serde_json::from_value(value).expect("Write msg should contain valid value");
            event_tx
                .send(Event::Cast(Command::KVWrite { key, value }))
                .await
                .expect("should be able to send Write event");

            send(
                event_tx.clone(),
                None,
                msg.src,
                Body::WriteOk {
                    msg_id: Some(
                        query(event_tx, |responder| Query::ReserveMsgId { responder }).await,
                    ),
                    in_reply_to: msg_id,
                },
            )
            .await
        }
        Body::Cas {
            msg_id,
            key,
            from,
            to,
        } => {
            let (tx, rx) = tokio::sync::oneshot::channel::<anyhow::Result<()>>();
            let from =
                serde_json::from_value(from).expect("cas from should have a deserializable value");
            let to = serde_json::from_value(to).expect("cas to should have a deserializable value");

            event_tx
                .send(Event::Call(Query::KVCas {
                    key,
                    from,
                    to,
                    responder: tx,
                }))
                .await
                .expect("should be able to send Cas event");

            let response = rx
                .await
                .expect("should be able to read from Cas oneshot channel");

            send(
                event_tx.clone(),
                None,
                msg.src,
                match response {
                    Ok(()) => Body::CasOk {
                        msg_id: Some(
                            query(event_tx, |responder| Query::ReserveMsgId { responder }).await,
                        ),
                        in_reply_to: msg_id,
                    },
                    Err(e) => match e.downcast_ref::<ErrorCode>() {
                        Some(e @ ErrorCode::PreconditionFailed)
                        | Some(e @ ErrorCode::KeyDoesNotExist) => Body::Error {
                            in_reply_to: msg_id,
                            code: e.clone(),
                            text: e.to_string(),
                        },
                        _ => panic!("encountered an unexpected error while processing Cas request"),
                    },
                },
            )
            .await
        }
        Body::RequestVote {
            msg_id,
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } => {
            todo!()
        }
        Body::RequestVoteOk { .. } => event_tx
            .send(Event::Cast(Command::ReceivedViaMaelstrom {
                response: msg.clone(),
            }))
            .await
            .unwrap(),
        Body::InitOk { .. }
        | Body::ReadOk { .. }
        | Body::WriteOk { .. }
        | Body::CasOk { .. }
        | Body::Error { .. } => panic!(),
    }
    ()
}

async fn broadcast(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    body: Body,
    responder: Option<tokio::sync::mpsc::Sender<Message>>,
) {
    let ids = query(event_tx.clone(), |responder| Query::GetOtherNodeIds {
        responder,
    })
    .await;
    let mut receivers: Vec<tokio::sync::oneshot::Receiver<Message>> = vec![];

    for id in ids {
        let (tx, rx) = tokio::sync::oneshot::channel();
        receivers.push(rx);
        send(event_tx.clone(), Some(tx), id, body.clone()).await;
    }

    let mut futures = receivers.into_iter().collect::<FuturesUnordered<_>>();

    tokio::spawn(async move {
        while let Some(result) = futures.next().await {
            match result {
                Ok(response) => {
                    if let Some(ref responder) = responder {
                        responder
                            .send(response)
                            .await
                            .expect("should be able to stream back a response to broadcast caller");
                    }
                }
                Err(_) => panic!(),
            }
        }
    });
}

pub async fn send(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    responder: Option<tokio::sync::oneshot::Sender<Message>>,
    dest: String,
    body: Body,
) {
    let (id_tx, id_rx) = tokio::sync::oneshot::channel::<String>();

    event_tx
        .send(Event::Call(Query::GetNodeId { responder: id_tx }))
        .await
        .unwrap();

    let message = Message {
        src: id_rx.await.unwrap(),
        dest: dest.to_string(),
        body: body.clone(),
    };

    let event = if let Some(responder) = responder {
        Event::Call(Query::SendViaMaelstrom { message, responder })
    } else {
        Event::Cast(Command::SendViaMaelstrom { message })
    };
    event_tx.send(event).await.unwrap();
}
