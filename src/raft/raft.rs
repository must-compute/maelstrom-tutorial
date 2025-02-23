use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use rand::Rng;

use super::{
    event::{query, Command, Event, Query},
    kv_store::KeyValueStore,
    log::Log,
    message::{Body, ErrorCode, Message},
};
use futures::stream::{FuturesUnordered, StreamExt};

pub type StateMachineKey = usize;
pub type StateMachineValue = usize;

type LeaderId = String;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Leader,
    Candidate,
    FollowerOf(LeaderId),
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
        let mut term = 0;
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
                    .expect("event handler should be able to send current term"),
                Event::Cast(Command::AdvanceTermTo { new_term }) => {
                    assert!(new_term > term);
                    term = new_term;
                }
                Event::Call(Query::GetOtherNodeIds { responder }) => {
                    responder
                        .send(other_node_ids.clone())
                        .expect("event handler should be able to send other node ids back");
                }
                Event::Call(Query::LastLogIndex { responder }) => responder
                    .send(log.len())
                    .expect("event handler should be able to send last log index"),
                Event::Call(Query::LastLogTerm { responder }) => responder
                    .send(log.last().term)
                    .expect("event handler should be able to send last log term"),
                Event::Cast(Command::BecomeFollowerOf { leader }) => {
                    node_state = NodeState::FollowerOf(leader)
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

    let (reset_election_deadline_tx, mut reset_election_deadline_rx) =
        tokio::sync::mpsc::channel::<()>(32);

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
            _ = reset_election_deadline_rx.recv() => {
                    let new_duration = Duration::from_millis(rng.random_range(1000..=3000));
                    let new_election_deadline =
                        tokio::time::interval_at(tokio::time::Instant::now() + new_duration, new_duration);
                    eprintln!(
                        "Timer elapsed, next interval: {}ms",
                        new_duration.as_millis()
                    );
                    election_deadline = new_election_deadline
                }
            _ = election_deadline.tick() =>  handle_election_tick(event_tx.clone(), reset_election_deadline_tx.clone()).await,
        }
    }
}

async fn handle_election_tick(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
) {
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
            let new_term = term + 1;
            event_tx
                .send(Event::Cast(Command::AdvanceTermTo { new_term }))
                .await
                .expect("should be able to send AdvanceTermTo event");
            eprintln!("became a candidate for {new_term}");

            request_votes(event_tx, reset_election_deadline_tx).await;
        }
        NodeState::Leader => {}
    };
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
        Body::RequestVote { .. } => {
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

// NOTE: overwrites msg_id because we don't have a nice abstraction for constructing msg bodies yet. TODO fix this.
async fn broadcast(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    responder: Option<tokio::sync::mpsc::Sender<Message>>,
    body: Body,
) {
    let ids = query(event_tx.clone(), |responder| Query::GetOtherNodeIds {
        responder,
    })
    .await;
    let mut receivers: Vec<tokio::sync::oneshot::Receiver<Message>> = vec![];

    for destination in ids {
        let (tx, rx) = tokio::sync::oneshot::channel();
        receivers.push(rx);
        let new_msg_id = query(event_tx.clone(), |responder| Query::ReserveMsgId {
            responder,
        })
        .await;
        let mut body = body.clone();
        body.set_msg_id(new_msg_id);
        send(event_tx.clone(), Some(tx), destination, body).await;
    }

    let mut responses = receivers.into_iter().collect::<FuturesUnordered<_>>();

    tokio::spawn(async move {
        while let Some(response_result) = responses.next().await {
            let response_message =
                response_result.expect("should be able to recv response during broadcast");
            if let Some(ref responder) = responder {
                responder
                    .send(response_message)
                    .await
                    .expect("should be able to return response message from broadcast()");
            }
        }
    });
}

async fn request_votes(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
) {
    let my_id = query(event_tx.clone(), |responder| Query::GetNodeId { responder }).await;
    let mut who_voted_for_me: HashSet<String> = Default::default();
    let my_term_before_the_election = query(event_tx.clone(), |responder| Query::CurrentTerm {
        responder,
    })
    .await;
    let last_log_index = query(event_tx.clone(), |responder| Query::LastLogIndex {
        responder,
    })
    .await;
    let last_log_term = query(event_tx.clone(), |responder| Query::LastLogTerm {
        responder,
    })
    .await;

    // TODO set chan size to node count in cluster
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(32);
    broadcast(
        event_tx.clone(),
        Some(tx),
        Body::RequestVote {
            msg_id: 0,
            term: my_term_before_the_election,
            candidate_id: my_id,
            last_log_index,
            last_log_term,
        },
    )
    .await;

    while let Some(response_message) = rx.recv().await {
        match response_message.body {
            Body::RequestVoteOk {
                term: voter_term,
                vote_granted,
                ..
            } => {
                // step down if needed
                if voter_term > my_term_before_the_election {
                    eprintln!(
                        "Stepping down: received term {voter_term} higher than my term {my_term_before_the_election}"
                    );

                    event_tx
                        .send(Event::Cast(Command::AdvanceTermTo {
                            new_term: voter_term,
                        }))
                        .await
                        .expect("should be able to cast AdvanceTermTo event");

                    become_follower(
                        event_tx.clone(),
                        reset_election_deadline_tx.clone(),
                        "TODO LEADER",
                        voter_term,
                    )
                    .await;
                }
                // record the vote if valid
                let state =
                    query(event_tx.clone(), |responder| Query::NodeState { responder }).await;
                let term = query(event_tx.clone(), |responder| Query::CurrentTerm {
                    responder,
                })
                .await;

                if matches!(state, NodeState::Candidate)
                    && term == my_term_before_the_election
                    && term == voter_term
                    && vote_granted
                {
                    who_voted_for_me.insert(response_message.src);
                    eprintln!("who voted for me: {:?}", who_voted_for_me);
                }
            }
            _ => panic!(
                "response to RequestVote should be RequestVoteOk. Got something else instead"
            ),
        };
    }
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

async fn become_follower(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
    leader: &str,
    term: usize,
) {
    // set node state to follower
    event_tx
        .send(Event::Cast(Command::BecomeFollowerOf {
            leader: leader.to_owned(),
        }))
        .await
        .expect("should be able to send BecomeFollower event in become_follower()");
    reset_election_deadline_tx
        .send(())
        .await
        .expect("should be able to reset election deadline when becoming a follower");
    eprintln!("became follower of {leader} in term {term}");
}
