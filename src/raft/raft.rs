use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use rand::Rng;

use crate::raft::event::update_raft_with;

use super::{
    event::{query, Command, Event, Query},
    kv_store::KeyValueStore,
    log::Log,
    message::{Body, ErrorCode, Message},
    raft_node::RaftNode,
};

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
        let mut raft = RaftNode {
            current_term: 0,
            log: Log::new(),
            my_id: Default::default(),
            node_state: NodeState::FollowerOf("TODO DETERMINE A SANE DEFAULT".to_string()),
            other_node_ids: Default::default(),
            voted_for: None,
        };

        let mut unacked: HashMap<usize, tokio::sync::oneshot::Sender<Message>> = Default::default();
        let mut next_msg_id = 0;
        let mut state_machine: KeyValueStore<StateMachineKey, StateMachineValue> =
            Default::default();

        while let Some(event) = event_rx.recv().await {
            match event {
                Event::Call(Query::RaftSnapshot { responder }) => {
                    responder
                        .send(raft.clone())
                        .expect("should be able to send node state");
                }
                Event::Cast(Command::Init { id, node_ids }) => {
                    raft.my_id = id;
                    raft.other_node_ids = node_ids
                        .into_iter()
                        .filter(|id| *id != raft.my_id)
                        .collect();
                }
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
                            tracing::debug!("inserted to unacked with key: {msg_id}");
                        }
                        _ => (),
                    };
                }
                Event::Cast(Command::ReceivedViaMaelstrom { response }) => {
                    tracing::debug!(
                        "handling Event::Cast(Command::ReceivedViaMaelstrom {:?}",
                        &response
                    );
                    // we received an ack, so we notify and remove from unacked.
                    if let Some(notifier) = unacked.remove(&response.body.in_reply_to()) {
                        tracing::debug!(
                            "popped unacked in_reply_to: {:?}",
                            &response.body.in_reply_to()
                        );
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
                Event::Cast(Command::SetRaftSnapshot(new_snapshot)) => raft = new_snapshot,
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
                    let reset_election_deadline_tx = reset_election_deadline_tx.clone();
                    async move {handle(event_tx, reset_election_deadline_tx, json_msg).await}
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
            _ = election_deadline.tick() =>  {
                tokio::spawn({
                    let event_tx = event_tx.clone();
                    let reset_election_deadline_tx = reset_election_deadline_tx.clone();
                    async move {
                        become_candidate(event_tx, reset_election_deadline_tx).await
                    }
                });
            }
        }
    }
}

async fn become_candidate(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
) {
    let RaftNode {
        node_state,
        my_id,
        current_term,
        ..
    } = query(event_tx.clone(), |responder| Query::RaftSnapshot {
        responder,
    })
    .await;

    match node_state {
        NodeState::Candidate | NodeState::FollowerOf(_) => {
            update_raft_with(event_tx.clone(), |raft| {
                raft.node_state = NodeState::Candidate;
            })
            .await;

            tracing::debug!("set my state to candidate");

            let new_term = current_term + 1;

            update_raft_with(event_tx.clone(), |raft| {
                raft.advance_term_to(new_term);
            })
            .await;

            tracing::debug!("advanced my term to {new_term}");

            tracing::debug!("set node I voted for to: myself");

            reset_election_deadline_tx
                .send(())
                .await
                .expect("should be able to reset election deadline when becoming a candidate");
            tracing::debug!("sent reset_election_deadline signal");
            tracing::debug!("I will call request_votes:");
            request_votes(event_tx, reset_election_deadline_tx).await;
            tracing::debug!("done calling request_votes");
            tracing::debug!("became a candidate for {new_term}");
        }
        NodeState::Leader => {}
    };
}

async fn handle(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
    msg: Message,
) -> () {
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
            term: candidate_term,
            candidate_id,
            last_log_index: candidate_last_log_index,
            last_log_term: candidate_last_log_term,
        } => {
            let RaftNode {
                current_term,
                voted_for,
                log,
                ..
            } = query(event_tx.clone(), |responder| Query::RaftSnapshot {
                responder,
            })
            .await;

            step_down_if_needed(
                event_tx.clone(),
                reset_election_deadline_tx.clone(),
                candidate_term,
                current_term,
            )
            .await;

            // in case my term was advanced by step_down_if_needed
            // TODO find a better design!!
            let raft @ RaftNode { current_term, .. } = query(event_tx.clone(), |responder| {
                Query::RaftSnapshot { responder }
            })
            .await;

            let mut vote_granted = false;

            if candidate_term < current_term {
                tracing::debug!("won't grant vote to a candidate whose term ({candidate_term}) is lower than my term ({current_term})");
            } else if voted_for.is_some() {
                tracing::debug!(
                    "won't grant vote, since I already voted for {:?}",
                    voted_for.unwrap()
                );
            } else if candidate_last_log_term < log.last_term() {
                tracing::debug!("won't grant vote to a candidate whose last log term ({candidate_last_log_term}) is older than my last log term ({})", log.last_term());
            } else if candidate_last_log_term == log.last_term()
                && candidate_last_log_index < log.len()
            {
                tracing::debug!("won't grant vote to candidate -- our logs are at the same term ({}) but their log index ({candidate_last_log_index}) is lower than mine ({})", log.last_term(), log.len());
            } else {
                tracing::debug!("granting vote to {candidate_id}");
                vote_granted = true;

                update_raft_with(event_tx.clone(), |raft| {
                    raft.set_voted_for(&candidate_id);
                })
                .await;

                reset_election_deadline_tx.send(()).await.expect(
                    "should be able to reset election deadline while handling a RequestVote msg",
                );
            }

            let new_msg_id = query(event_tx.clone(), |responder| Query::ReserveMsgId {
                responder,
            })
            .await;

            send(
                event_tx,
                None,
                msg.src,
                Body::RequestVoteOk {
                    msg_id: new_msg_id,
                    in_reply_to: msg_id,
                    term: current_term,
                    vote_granted,
                },
            )
            .await;
            tracing::debug!("called send(RequestVoteOk) from handler of RequestVote");
        }
        Body::RequestVoteOk { .. } => {
            tracing::debug!(
                "RECEIVED Body::RequestVoteOk. Sending Event::Cast(Command::ReceivedViaMaelstrom)"
            );
            event_tx
                .send(Event::Cast(Command::ReceivedViaMaelstrom {
                    response: msg.clone(),
                }))
                .await
                .unwrap();
            tracing::debug!("DONE Sending Event::Cast(Command::ReceivedViaMaelstrom)");
        }
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
    let RaftNode { other_node_ids, .. } = query(event_tx.clone(), |responder| {
        Query::RaftSnapshot { responder }
    })
    .await;
    let mut receiver_tasks = tokio::task::JoinSet::<Message>::new();

    for destination in other_node_ids {
        let (tx, rx) = tokio::sync::oneshot::channel::<Message>();
        receiver_tasks.spawn(async move {
            rx.await
                .expect("should be able to recv on one of the broadcast responses")
        });
        let new_msg_id = query(event_tx.clone(), |responder| Query::ReserveMsgId {
            responder,
        })
        .await;
        let mut body = body.clone();
        body.set_msg_id(new_msg_id);
        send(event_tx.clone(), Some(tx), destination, body).await;
    }

    // TODO should this really be in a separate task?
    tokio::spawn(async move {
        tracing::debug!("spawned a task at the end of broadcast. Awaiting responses");
        while let Some(response_result) = receiver_tasks.join_next().await {
            let response_message =
                response_result.expect("should be able to recv response during broadcast");
            tracing::debug!(
                "got a response message to broadcast (within the async task). Forwarding it"
            );
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
    let RaftNode {
        my_id,
        other_node_ids,
        log,
        current_term: my_term_before_the_election,
        ..
    } = query(event_tx.clone(), |responder| Query::RaftSnapshot {
        responder,
    })
    .await;

    let mut who_voted_for_me: HashSet<String> = Default::default();

    tracing::debug!("i'm now requesting votes. broadcasting request_vote msg");

    let (tx, mut rx) = tokio::sync::mpsc::channel::<Message>(other_node_ids.len());
    broadcast(
        event_tx.clone(),
        Some(tx),
        Body::RequestVote {
            msg_id: 0,
            term: my_term_before_the_election,
            candidate_id: my_id,
            last_log_index: log.len(),
            last_log_term: log.last_term(),
        },
    )
    .await;

    tracing::debug!("i'm done broadcasting request_vote msg. I'll loop over the responses");

    let mut remaining_response_count = other_node_ids.len();
    while let Some(response_message) = rx.recv().await {
        tracing::debug!("remaining_response_count: {remaining_response_count}");
        match response_message.body {
            Body::RequestVoteOk {
                term: voter_term,
                vote_granted,
                ..
            } => {
                step_down_if_needed(
                    event_tx.clone(),
                    reset_election_deadline_tx.clone(),
                    voter_term,
                    my_term_before_the_election,
                )
                .await;

                // record the vote if valid
                let raft = query(event_tx.clone(), |responder| Query::RaftSnapshot {
                    responder,
                })
                .await;

                if matches!(raft.node_state, NodeState::Candidate)
                    && raft.current_term == my_term_before_the_election
                    && raft.current_term == voter_term
                    && vote_granted
                {
                    who_voted_for_me.insert(response_message.src);
                    tracing::debug!("who voted for me: {:?}", who_voted_for_me);
                }
            }
            _ => {
                tracing::error!(
                    "Error: broadcasted RequestVote but received body that isn't RequestVoteOk"
                );
                panic!(
                    "response to RequestVote should be RequestVoteOk. Got something else instead"
                );
            }
        };
        remaining_response_count -= 1;
        if remaining_response_count == 0 {
            break;
        }
    }

    tracing::debug!("I'm done looping over the responses to broadcast of request_votes");
}

async fn step_down_if_needed(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
    voter_term: usize,
    my_term_before_the_election: usize,
) {
    if voter_term > my_term_before_the_election {
        eprintln!(
            "Stepping down: received term {voter_term} higher than my term {my_term_before_the_election}"
        );

        update_raft_with(event_tx.clone(), |raft| {
            raft.advance_term_to(voter_term);
        })
        .await;

        become_follower(
            event_tx.clone(),
            reset_election_deadline_tx.clone(),
            "TODO LEADER",
            voter_term,
        )
        .await;
    }
}

pub async fn send(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    responder: Option<tokio::sync::oneshot::Sender<Message>>,
    dest: String,
    body: Body,
) {
    let RaftNode { my_id, .. } = query(event_tx.clone(), |responder| Query::RaftSnapshot {
        responder,
    })
    .await;

    let message = Message {
        src: my_id,
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
    update_raft_with(event_tx, |raft| {
        raft.become_follower_of(leader);
    })
    .await;

    tracing::debug!("I set my state to follower of {leader}");
    reset_election_deadline_tx
        .send(())
        .await
        .expect("should be able to reset election deadline when becoming a follower");
    tracing::debug!("I reset the election deadline because I set state to follower of {leader}");
    eprintln!("became follower of {leader} in term {term}");
}
