use std::{
    cmp::{max, min},
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use rand::Rng;

use super::{
    event::{Command, Event, Query},
    log::Entry,
    message::{Body, ErrorCode, Message},
    raft_node::RaftNode,
};

pub(super) type LeaderId = String;

#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Leader,
    Candidate,
    FollowerOf(Option<LeaderId>),
}

pub async fn run() {
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Event>(32);
    let raft: RaftNode = Default::default();

    // event handler task
    tokio::spawn(async move {
        let mut unacked: HashMap<usize, tokio::sync::oneshot::Sender<Message>> = Default::default();

        while let Some(event) = event_rx.recv().await {
            match event {
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
    let (reset_replication_deadline_tx, mut reset_replication_deadline_rx) =
        tokio::sync::mpsc::channel::<()>(32);

    let mut rng = rand::rng();
    let start = tokio::time::Instant::now() + Duration::from_millis(rng.random_range(100..=300));
    let mut election_deadline = tokio::time::interval_at(start, Duration::from_millis(100));
    let mut replication_deadline = tokio::time::interval(Duration::from_millis(50));
    // TODO how much time???
    let raft_node = Arc::new(raft);

    loop {
        tokio::select! {
            Some(json_msg) = stdin_rx.recv() => {
                tokio::spawn({
                    let event_tx = event_tx.clone();
                    let reset_election_deadline_tx = reset_election_deadline_tx.clone();
                    let raft_node = raft_node.clone();
                    async move {handle(event_tx, reset_election_deadline_tx, raft_node, json_msg).await}
                });
            }
            _ = reset_election_deadline_rx.recv() => {
                    let new_duration = Duration::from_millis(rng.random_range(100..=300));
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
                    let raft_node = raft_node.clone();
                    async move {
                        become_candidate(event_tx, reset_election_deadline_tx, raft_node).await
                    }
                });
            }
            _ = reset_replication_deadline_rx.recv() => {
                    replication_deadline.reset(); // TODO probably use the same method in election resetting?
                }
            _ = replication_deadline.tick() => {
                tokio::spawn({
                    let event_tx = event_tx.clone();
                    let reset_election_deadline_tx = reset_election_deadline_tx.clone();
                    let reset_replication_deadline_tx = reset_replication_deadline_tx.clone();
                    let raft_node = raft_node.clone();
                    async move {
                        if *raft_node.node_state.lock().unwrap() == NodeState::Leader {
                            replicate_log(event_tx, reset_election_deadline_tx, reset_replication_deadline_tx, raft_node).await;
                        }
                    }
                });
            }
        }
    }
}

async fn become_candidate(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
    raft_node: Arc<RaftNode>,
) {
    let node_state = raft_node.node_state.lock().unwrap().clone();
    match node_state {
        NodeState::Candidate | NodeState::FollowerOf(_) => {
            *raft_node.node_state.lock().unwrap() = NodeState::Candidate;

            tracing::debug!("set my state to candidate");

            let existing_term = raft_node.current_term.fetch_add(1, Ordering::SeqCst);
            let new_term = existing_term + 1; // ugh. I'll fix this when i couple term updates to nodestate updates.

            tracing::debug!(
                "advanced my term from {existing_term} to {:?}",
                existing_term + 1
            );

            tracing::debug!("set node I voted for to: myself"); // TODO ?????

            reset_election_deadline_tx
                .send(())
                .await
                .expect("should be able to reset election deadline when becoming a candidate");
            tracing::debug!("sent reset_election_deadline signal");
            tracing::debug!("I will call request_votes:");
            request_votes(event_tx, reset_election_deadline_tx, raft_node.clone()).await;
            tracing::debug!("done calling request_votes");
            tracing::debug!("became a candidate for {new_term}");
        }
        NodeState::Leader => {}
    };
}

async fn handle(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
    raft_node: Arc<RaftNode>,
    msg: Message,
) -> () {
    match msg.body {
        Body::Init {
            msg_id,
            node_id,
            node_ids,
        } => {
            *raft_node.my_id.lock().unwrap() = node_id.clone();
            *raft_node.other_node_ids.lock().unwrap() =
                node_ids.into_iter().filter(|id| *id != node_id).collect();

            send(
                event_tx.clone(),
                None,
                raft_node.clone(),
                msg.src,
                Body::InitOk {
                    msg_id: Some(raft_node.reserve_next_msg_id()),
                    in_reply_to: msg_id,
                },
            )
            .await;
        }
        Body::RequestVote {
            msg_id,
            term: candidate_term,
            candidate_id,
            last_log_index: candidate_last_log_index,
            last_log_term: candidate_last_log_term,
        } => {
            step_down_if_needed(
                event_tx.clone(),
                reset_election_deadline_tx.clone(),
                raft_node.clone(),
                candidate_term,
            )
            .await;

            // in case my term was advanced by step_down_if_needed
            let mut vote_granted = false;
            let log = raft_node.log.lock().unwrap().clone();
            let current_term = raft_node.current_term.load(Ordering::SeqCst);
            let voted_for = raft_node.voted_for.lock().unwrap().clone();

            if candidate_term < current_term {
                tracing::debug!("won't grant vote to a candidate whose term ({candidate_term}) is lower than my term ({current_term})");
            } else if voted_for.is_some() {
                tracing::debug!(
                    "won't grant vote, since I already voted for {:?}",
                    voted_for
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

                *raft_node.voted_for.lock().unwrap() = Some(candidate_id);

                reset_election_deadline_tx.send(()).await.expect(
                    "should be able to reset election deadline while handling a RequestVote msg",
                );
            }

            send(
                event_tx,
                None,
                raft_node.clone(),
                msg.src,
                Body::RequestVoteOk {
                    msg_id: raft_node.reserve_next_msg_id(),
                    in_reply_to: msg_id,
                    term: current_term,
                    vote_granted,
                },
            )
            .await;
            tracing::debug!("called send(RequestVoteOk) from handler of RequestVote");
        }
        Body::AppendEntries {
            msg_id,
            term: leader_term,
            leader_id,
            prev_log_index,
            prev_log_term,
            mut entries,
            leader_commit,
        } => {
            step_down_if_needed(
                event_tx.clone(),
                reset_election_deadline_tx.clone(),
                raft_node.clone(),
                leader_term,
            )
            .await;

            let my_term = raft_node.current_term.load(Ordering::SeqCst);
            let response_body = Body::AppendEntriesOk {
                msg_id: raft_node.reserve_next_msg_id(),
                in_reply_to: msg_id,
                term: my_term,
                success: false,
            };

            if leader_term < my_term {
                send(
                    event_tx.clone(),
                    None,
                    raft_node.clone(),
                    msg.src.clone(),
                    response_body,
                )
                .await;
                return;
            }

            // i am follower, and now i now know the leader.
            assert!(matches!(
                *raft_node.node_state.lock().unwrap(),
                NodeState::FollowerOf(..)
            ));
            *raft_node.node_state.lock().unwrap() = NodeState::FollowerOf(Some(leader_id));
            reset_election_deadline_tx.send(()).await.expect("should be able to reset election deadline when receiving AppendEntriesRPC from leader");

            if prev_log_index <= 0 {
                send(
                    event_tx.clone(),
                    None,
                    raft_node.clone(),
                    msg.src.clone(),
                    Body::Error {
                        in_reply_to: msg_id,
                        code: ErrorCode::Abort,
                        text: format!("out of bounds prev_log_index: {prev_log_index}"),
                    },
                )
                .await;
            }

            let mut should_reject_request = false;
            {
                let log_guard = raft_node.log.lock().unwrap();
                let prev_entry = log_guard.get(prev_log_index);
                should_reject_request =
                    prev_entry.is_none() || prev_entry.is_some_and(|e| e.term != prev_log_term);
            }

            if should_reject_request {
                send(
                    event_tx.clone(),
                    None,
                    raft_node.clone(),
                    msg.src.clone(),
                    response_body,
                )
                .await;
                return;
            }

            // at this point we can conclude that we agree with the leader on the previous term
            {
                let mut log_guard = raft_node.log.lock().unwrap();
                log_guard.discard_after(prev_log_index);
                log_guard.append(&mut entries);
            }

            if leader_commit > raft_node.commit_index.load(Ordering::SeqCst) {
                let my_new_commit_index = min(leader_commit, raft_node.log.lock().unwrap().len());
                raft_node
                    .commit_index
                    .store(my_new_commit_index, Ordering::SeqCst);
            }

            // TODO make the response code below more concise.
            let Body::AppendEntriesOk {
                msg_id,
                in_reply_to,
                term,
                ..
            } = response_body
            else {
                unreachable!()
            };

            let response_body = Body::AppendEntriesOk {
                msg_id,
                in_reply_to,
                term,
                success: true,
            };

            send(
                event_tx.clone(),
                None,
                raft_node.clone(),
                msg.src.clone(),
                response_body,
            )
            .await;
        }
        Body::Read { .. } | Body::Write { .. } | Body::Cas { .. } => {
            let node_state = raft_node.node_state.lock().unwrap().clone();
            match node_state {
                NodeState::Leader => {
                    raft_node.log.lock().unwrap().append(&mut vec![Entry {
                        term: raft_node.current_term.load(Ordering::SeqCst),
                        op: Some(msg.clone()),
                    }]);

                    // TODO shouldn't this be guarded by quorum?
                    apply_to_state_machine(event_tx.clone(), raft_node.clone(), msg).await;
                }
                NodeState::FollowerOf(Some(leader_id)) => {
                    // act as a proxy between the leader and the client.

                    let client_msg_id = msg.body.msg_id();

                    let mut body_to_forward = msg.body.clone();
                    body_to_forward.set_msg_id(raft_node.reserve_next_msg_id());

                    let (tx, rx) = tokio::sync::oneshot::channel::<Message>();
                    send(
                        event_tx.clone(),
                        Some(tx),
                        raft_node.clone(),
                        leader_id,
                        body_to_forward,
                    )
                    .await;

                    let mut response = rx
                        .await
                        .expect("should proxy response to msg between client and leader");

                    response.body.set_msg_id(client_msg_id);

                    send(
                        event_tx.clone(),
                        None,
                        raft_node.clone(),
                        msg.src,
                        response.body,
                    )
                    .await;
                }
                NodeState::FollowerOf(None) | NodeState::Candidate => {
                    let body = Body::Error {
                        in_reply_to: msg.body.msg_id(),
                        code: ErrorCode::TemporarilyUnavailable,
                        text: "can't read/write/cas from non-leader node".to_string(),
                    };
                    send(
                        event_tx.clone(),
                        None,
                        raft_node.clone(),
                        msg.src.clone(),
                        body,
                    )
                    .await;
                }
            }
        }
        Body::InitOk { .. }
        | Body::ReadOk { .. }
        | Body::WriteOk { .. }
        | Body::CasOk { .. }
        | Body::Error { .. }
        | Body::RequestVoteOk { .. }
        | Body::AppendEntriesOk { .. } => {
            event_tx
                .send(Event::Cast(Command::ReceivedViaMaelstrom {
                    response: msg.clone(),
                }))
                .await
                .unwrap();
        }
    }
    ()
}

// NOTE: overwrites msg_id because we don't have a nice abstraction for constructing msg bodies yet. TODO fix this.
async fn broadcast(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    responder: Option<tokio::sync::mpsc::Sender<Message>>,
    raft_node: Arc<RaftNode>,
    body: Body,
) {
    let mut receiver_tasks = tokio::task::JoinSet::<Message>::new();

    let other_node_ids = raft_node.other_node_ids.lock().unwrap().clone();
    for destination in other_node_ids {
        let (tx, rx) = tokio::sync::oneshot::channel::<Message>();
        receiver_tasks.spawn(async move {
            rx.await
                .expect("should be able to recv on one of the broadcast responses")
        });

        let new_msg_id = raft_node.reserve_next_msg_id();

        let mut body = body.clone();
        body.set_msg_id(new_msg_id);
        send(
            event_tx.clone(),
            Some(tx),
            raft_node.clone(),
            destination,
            body,
        )
        .await;
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
    raft_node: Arc<RaftNode>,
) {
    let mut who_voted_for_me: HashSet<String> =
        HashSet::from([raft_node.my_id.lock().unwrap().to_owned()]);

    tracing::debug!("i'm now requesting votes. broadcasting request_vote msg");

    let my_term_before_the_election = raft_node.current_term.load(Ordering::SeqCst);
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<Message>(raft_node.other_node_ids.lock().unwrap().len());

    let candidate_id = raft_node.my_id.lock().unwrap().to_owned();
    let last_log_index = raft_node.log.lock().unwrap().len();
    let last_log_term = raft_node.log.lock().unwrap().last_term();

    broadcast(
        event_tx.clone(),
        Some(tx),
        raft_node.clone(),
        Body::RequestVote {
            msg_id: 0,
            term: my_term_before_the_election,
            candidate_id,
            last_log_index,
            last_log_term,
        },
    )
    .await;

    tracing::debug!("i'm done broadcasting request_vote msg. I'll loop over the responses");

    let mut remaining_response_count = raft_node.other_node_ids.lock().unwrap().len();
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
                    raft_node.clone(),
                    voter_term,
                )
                .await;

                // record the vote if valid
                if matches!(*raft_node.node_state.lock().unwrap(), NodeState::Candidate)
                    && raft_node.current_term.load(Ordering::SeqCst) == my_term_before_the_election
                    && raft_node.current_term.load(Ordering::SeqCst) == voter_term
                    && vote_granted
                {
                    who_voted_for_me.insert(response_message.src);
                    tracing::debug!("who voted for me: {:?}", who_voted_for_me);

                    if who_voted_for_me.len() >= raft_node.majority_count() {
                        become_leader(
                            event_tx.clone(),
                            reset_election_deadline_tx.clone(),
                            raft_node.clone(),
                        )
                        .await;
                    }
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
    raft_node: Arc<RaftNode>,
    voter_term: usize,
) {
    let my_term_before_the_election = raft_node.current_term.load(Ordering::SeqCst);
    if voter_term > my_term_before_the_election {
        eprintln!(
            "Stepping down: received term {voter_term} higher than my term {my_term_before_the_election}"
        );

        raft_node.advance_term_to(voter_term);

        become_follower(
            reset_election_deadline_tx.clone(),
            raft_node.clone(),
            None,
            voter_term,
        )
        .await;
    }
}

async fn send(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    responder: Option<tokio::sync::oneshot::Sender<Message>>,
    raft_node: Arc<RaftNode>,
    dest: String,
    body: Body,
) {
    let message = Message {
        src: raft_node.my_id.lock().unwrap().to_owned(),
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
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
    raft_node: Arc<RaftNode>,
    leader: Option<LeaderId>,
    term: usize,
) {
    *raft_node.node_state.lock().unwrap() = NodeState::FollowerOf(leader.clone());
    *raft_node.match_index.lock().unwrap() = HashMap::new();
    *raft_node.next_index.lock().unwrap() = HashMap::new();

    reset_election_deadline_tx
        .send(())
        .await
        .expect("should be able to reset election deadline when becoming a follower");
    tracing::debug!(
        "I reset the election deadline because I set state to follower of {:?}",
        leader
    );
    tracing::debug!("became follower of {:?} in term {term}", leader);
}

async fn become_leader(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
    raft_node: Arc<RaftNode>,
) {
    let mut node_state_guard = raft_node.node_state.lock().unwrap();
    assert!(matches!(*node_state_guard, NodeState::Candidate));
    *node_state_guard = NodeState::Leader;

    //todo replicate
    let mut next_index = raft_node.next_index.lock().unwrap();
    let mut match_index = raft_node.match_index.lock().unwrap();

    *next_index = Default::default();
    *match_index = Default::default();

    let other_node_ids = raft_node.other_node_ids.lock().unwrap().clone();
    let log_len = raft_node.log.lock().unwrap().len().clone();
    for node_id in other_node_ids {
        next_index.insert(node_id.clone(), log_len + 1);
        match_index.insert(node_id, 0);
    }

    //TODO - kyle has a reset_step_down_deadline thingy here

    tracing::debug!(
        "became leader for term {:?}",
        raft_node.current_term.load(Ordering::SeqCst)
    );
}

async fn apply_to_state_machine(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    raft_node: Arc<RaftNode>,
    msg: Message,
) {
    match msg.body {
        Body::Read { msg_id, key } => {
            let maybe_value = raft_node
                .state_machine
                .lock()
                .unwrap()
                .read(&key)
                .map(|v| v.to_owned());

            let body = match maybe_value {
                Some(value) => Body::ReadOk {
                    msg_id: Some(raft_node.reserve_next_msg_id()),
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
            };

            send(event_tx.clone(), None, raft_node.clone(), msg.src, body).await
        }
        Body::Write { msg_id, key, value } => {
            let value =
                serde_json::from_value(value).expect("Write msg should contain valid value");

            raft_node.state_machine.lock().unwrap().write(key, value);

            send(
                event_tx.clone(),
                None,
                raft_node.clone(),
                msg.src,
                Body::WriteOk {
                    msg_id: Some(raft_node.reserve_next_msg_id()),
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
            let from =
                serde_json::from_value(from).expect("cas from should have a deserializable value");
            let to = serde_json::from_value(to).expect("cas to should have a deserializable value");

            let cas_result = raft_node.state_machine.lock().unwrap().cas(key, from, to);
            let body = match cas_result {
                Ok(()) => Body::CasOk {
                    msg_id: Some(raft_node.reserve_next_msg_id()),
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
            };

            send(event_tx.clone(), None, raft_node.clone(), msg.src, body).await
        }
        _ => panic!(
            "apply_to_state_machine expects a Read/Write/Cas msg but it was given something else."
        ),
    }
}

async fn replicate_log(
    event_tx: tokio::sync::mpsc::Sender<Event>,
    reset_election_deadline_tx: tokio::sync::mpsc::Sender<()>,
    reset_replication_deadline_tx: tokio::sync::mpsc::Sender<()>,
    raft_node: Arc<RaftNode>,
) {
    let current_term = raft_node.current_term.load(Ordering::SeqCst);
    let my_id = raft_node.my_id.lock().unwrap().clone();
    let other_node_ids = raft_node.other_node_ids.lock().unwrap().clone();

    let next_index = raft_node.next_index.lock().unwrap().clone();
    let log = raft_node.log.lock().unwrap().clone();
    let leader_commit = raft_node.commit_index.load(Ordering::SeqCst);
    tracing::debug!(
        "wwwwwww next_index: {:?}. Log len: {:?}",
        next_index,
        log.len()
    );

    for node in other_node_ids {
        let next_index_to_send = *next_index.get(&node).unwrap_or(&0);
        tracing::debug!(
            "vvvvvvv next_index_to_send: {next_index_to_send}. Log len: {:?}",
            log.len()
        );

        let entries = log.from_index_till_end(next_index_to_send);

        let prev_log_index = next_index_to_send - 1;
        let prev_log_term = log.get(prev_log_index).map_or(0, |entry| entry.term);

        let (tx, rx) = tokio::sync::oneshot::channel::<Message>();
        let body = Body::AppendEntries {
            msg_id: raft_node.reserve_next_msg_id(),
            term: current_term,
            leader_id: my_id.clone(),
            prev_log_index,
            prev_log_term,
            entries: entries.into(),
            leader_commit,
        };
        send(
            event_tx.clone(),
            Some(tx),
            raft_node.clone(),
            node.clone(),
            body,
        )
        .await;

        let response = rx
            .await
            .expect("should receive a response to AppendEntries msg");

        match response.body {
            Body::AppendEntriesOk {
                success,
                term: voter_term,
                ..
            } => {
                step_down_if_needed(
                    event_tx.clone(),
                    reset_election_deadline_tx.clone(),
                    raft_node.clone(),
                    voter_term,
                )
                .await;
                if success {
                    let mut next_index_guard = raft_node.next_index.lock().unwrap();
                    let updated_next_index_entry = max(
                        *next_index_guard.get(&node).unwrap_or(&0),
                        next_index_to_send + entries.len(),
                    );
                    next_index_guard.insert(node.clone(), updated_next_index_entry);
                    std::mem::drop(next_index_guard);

                    let mut match_index_guard = raft_node.match_index.lock().unwrap();
                    let updated_match_index_entry = max(
                        *match_index_guard.get(&node).unwrap_or(&0),
                        next_index_to_send + entries.len() - 1,
                    );
                    match_index_guard.insert(node.clone(), updated_match_index_entry);
                    std::mem::drop(match_index_guard);

                    tracing::debug!(
                        "successfully replicated to {node}. Next index: {:?}",
                        raft_node.next_index.lock().unwrap()
                    );
                } else {
                    // replication request was rejected. We can infer it's due to an index mismatch, so we decrement
                    // the next index to send (for the future re-attempt of replication).
                    raft_node
                        .next_index
                        .lock()
                        .unwrap()
                        .entry(node)
                        .and_modify(|next_index| *next_index -= 1);
                }
            }
            _ => panic!("expected AppendEntriesOk in response to AppendEntries"),
        };

        // TODO I don't know if this is the right place for reset
        reset_replication_deadline_tx
            .send(())
            .await
            .expect("should be able to reset replication deadline after replication");
    }
}
