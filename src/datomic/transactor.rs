use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use crate::datomic::message::{Body, ErrorCode};
use anyhow::Result;
use tokio::sync::mpsc::{Receiver, Sender};

use super::{
    event::{Command, Event, Query},
    message::{Message, Transaction},
    micro_op::MicroOperation,
    thunk::{Thunk, ThunkIdGen, ThunkValue},
};

#[derive(Debug)]
enum DatomicError {
    TransactionConflict,
}

impl std::fmt::Display for DatomicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatomicError::TransactionConflict => write!(f, "Transaction failed"),
        }
    }
}

impl std::error::Error for DatomicError {}

#[derive(Debug)]
pub struct Node {
    event_tx: Sender<Event>,
    pub kv_store: String,
    root_created: AtomicBool,
    // pub id: String,
    // pub node_ids: Vec<String>, // Every node in the network (including this node)
}

impl Node {
    fn new() -> (Self, Receiver<Event>) {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel::<Event>(32);
        (
            Self {
                event_tx,
                kv_store: "lww-kv".to_string(),
                root_created: false.into(),
            },
            event_rx,
        )
    }

    async fn run(self, mut event_rx: Receiver<Event>, thunk_id_generator: ThunkIdGen) {
        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);

        // event handler task
        tokio::spawn(async move {
            let mut unacked: HashMap<usize, tokio::sync::oneshot::Sender<Message>> =
                Default::default();
            let mut node_id: String = Default::default();
            // let mut _ids: Vec<String> = Default::default();
            let mut next_msg_id = 0;

            while let Some(event) = event_rx.recv().await {
                match event {
                    Event::Cast(Command::Init { id, .. }) => {
                        node_id = id;
                        // _ids = node_ids;
                    }
                    Event::Call(Query::GetNodeId { responder }) => responder
                        .send(node_id.clone())
                        .expect("respond to Query::GetNodeId"),
                    Event::Call(Query::SendViaMaelstrom { ref message, .. })
                    | Event::Cast(Command::SendViaMaelstrom { ref message }) => {
                        let mut message = message.clone();
                        // if !matches!(message.body, Body::TxnOk { .. })
                        //     && !matches!(message.body, Body::Error { .. })
                        // {
                        //     message.body.set_msg_id(next_msg_id);
                        //     next_msg_id += 1;
                        // }
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

        let node = Arc::new(self);
        let thunk_id_generator = Arc::new(thunk_id_generator);

        loop {
            tokio::select! {
                Some(json_msg) = stdin_rx.recv() => {
                    tokio::spawn({
                        let node = node.clone();
                        let thunk_id_generator = thunk_id_generator.clone();
                        async move {node.handle(thunk_id_generator, &json_msg).await}
                    });
                }
                //...
            }
        }
    }

    // TODO this can now be a free function (now that Node is useless)
    // #[tracing::instrument]
    async fn handle(&self, thunk_id_generator: Arc<ThunkIdGen>, msg: &Message) {
        match &msg.body {
            Body::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                self.event_tx
                    .send(Event::Cast(Command::Init {
                        id: node_id.clone(),
                        node_ids: node_ids.clone(),
                    }))
                    .await
                    .unwrap();

                self.send(
                    None,
                    &msg.src,
                    &Body::InitOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                    },
                )
                .await;
            }
            Body::Txn { txn, .. } => {
                tracing::debug!("handle() received Body::Txn");
                let txn_result = self.transact(thunk_id_generator, txn.clone()).await;

                match txn_result {
                    Ok(completed_txn) => {
                        self.send(
                            None,
                            &msg.src,
                            &Body::TxnOk {
                                txn: completed_txn,
                                in_reply_to: msg.body.msg_id(),
                            },
                        )
                        .await
                    }
                    Err(e) => match e.downcast_ref::<DatomicError>() {
                        Some(DatomicError::TransactionConflict) => {
                            self.send(
                                None,
                                &msg.src,
                                &Body::Error {
                                    in_reply_to: msg.body.msg_id(),
                                    code: ErrorCode::TxnConflict,
                                    text: "CAS failed!".to_string(),
                                },
                            )
                            .await;
                        }
                        None => panic!("unexpected txn failure"),
                    },
                };
            }
            _ => {
                self.event_tx
                    .send(Event::Cast(Command::ReceivedViaMaelstrom {
                        response: msg.clone(),
                    }))
                    .await
                    .unwrap();
            }
        }
    }

    pub async fn send(
        &self,
        responder: Option<tokio::sync::oneshot::Sender<Message>>,
        dest: &str,
        body: &Body,
    ) {
        let (id_tx, id_rx) = tokio::sync::oneshot::channel::<String>();

        self.event_tx
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
        self.event_tx.send(event).await.unwrap();
    }

    // blocks until obtaining the response Message
    pub async fn sync_rpc(&self, dest: &str, msg_body: &Body) -> Message {
        let (notifier_tx, notifier_rx) = tokio::sync::oneshot::channel::<Message>();
        self.send(Some(notifier_tx), dest, msg_body).await;
        let response = notifier_rx.await.unwrap();
        response
    }

    pub async fn reserve_msg_id(&self) -> usize {
        let (tx, rx) = tokio::sync::oneshot::channel::<usize>();
        self.event_tx
            .send(Event::Call(Query::ReserveMsgId { responder: tx }))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    async fn node_id(&self) -> String {
        let (tx, rx) = tokio::sync::oneshot::channel::<String>();
        self.event_tx
            .send(Event::Call(Query::GetNodeId { responder: tx }))
            .await
            .unwrap();
        rx.await.unwrap()
    }

    #[tracing::instrument]
    pub async fn transact(
        &self,
        thunk_id_generator: Arc<ThunkIdGen>,
        mut txn: Transaction,
    ) -> Result<Transaction> {
        tracing::debug!("transact() was called");
        let root = String::from("ROOT");
        let node_id = self.node_id().await;

        let read_msg_body = Body::Read {
            msg_id: self.reserve_msg_id().await,
            key: root.clone(),
        };

        let kv_thunk: Thunk;
        loop {
            let response = self.sync_rpc(&self.kv_store, &read_msg_body).await;
            match response.body {
                Body::ReadOk { value, .. } => {
                    kv_thunk = serde_json::from_value(value)
                        .expect("kv store ReadOk should return a ThunkValue");
                    break;
                }
                Body::Error { ref code, .. } => {
                    tracing::debug!("RECEIVED RESPONSE, CONTAINS ERROR");
                    if *code == ErrorCode::KeyDoesNotExist {
                        if self.node_id().await == "n0" && !self.root_created.load(Ordering::SeqCst)
                        {
                            let mut initial_root_value = Thunk::new(
                                thunk_id_generator.generate(&node_id),
                                ThunkValue::Intermediate(Default::default()),
                            );

                            initial_root_value.store(self).await;

                            let response = self
                                .sync_rpc(
                                    &self.kv_store,
                                    &Body::Write {
                                        msg_id: self.reserve_msg_id().await,
                                        key: root.clone(),
                                        value: serde_json::to_value(initial_root_value.clone())
                                            .unwrap(),
                                    },
                                )
                                .await;

                            if matches!(response.body, Body::WriteOk { .. }) {
                                self.root_created.store(true, Ordering::SeqCst);
                            } else {
                                panic!();
                            }
                            kv_thunk = initial_root_value;
                            break;
                        } else {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    } else {
                        panic!("unexpected error code while reading from kv store");
                    }
                }
                _ => panic!("something went wrong while reading from kv store"),
            };
        }

        let mut local_snapshot = kv_thunk.clone();

        let ThunkValue::Intermediate(ref mut local_map) = local_snapshot.value(self).await else {
            panic!("expected a map thunk on the first read")
        };

        for op in txn.iter_mut() {
            match op {
                MicroOperation::Read { key, value } => {
                    if let Some(mut thunk) = local_map.get(key).map(|v| v.to_owned()) {
                        let ThunkValue::Ultimate(ultimate_value) = thunk.value(self).await else {
                            panic!("reading a key from a map thunk should return an ultimate value")
                        };
                        *value = Some(ultimate_value);
                    }
                }
                MicroOperation::Append { key, value } => {
                    let new_value = if let Some(thunk) = local_map.get_mut(key) {
                        let ThunkValue::Ultimate(ref mut ultimate_value) = thunk.value(self).await
                        else {
                            panic!("reading a key from a map thunk should return an ultimate value")
                        };
                        ultimate_value.push(*value);
                        ultimate_value.clone()
                    } else {
                        vec![*value]
                    };

                    let new_thunk = Thunk::new(
                        thunk_id_generator.generate(&node_id),
                        ThunkValue::Ultimate(new_value),
                    );

                    local_map.insert(*key, new_thunk);
                }
            }
        }

        let mut new_thunk = Thunk::new(
            thunk_id_generator.generate(&node_id),
            ThunkValue::Intermediate(local_map.clone()),
        );
        new_thunk.store(self).await;

        // cas
        let cas_msg_body = Body::Cas {
            msg_id: self.reserve_msg_id().await,
            key: root.clone(),
            from: serde_json::to_value(kv_thunk).unwrap(),
            to: serde_json::to_value(new_thunk).unwrap(),
            create_if_not_exists: true, // only supported by lin-kv
        };

        loop {
            let response = self.sync_rpc(&self.kv_store, &cas_msg_body).await;
            match response.body {
                Body::CasOk { .. } => return Ok(txn),
                Body::Error {
                    code: ErrorCode::PreconditionFailed,
                    ..
                } => {
                    let err = anyhow::Error::new(DatomicError::TransactionConflict);
                    return Err(err);
                }
                Body::Error {
                    code: ErrorCode::KeyDoesNotExist,
                    ..
                } => tokio::time::sleep(Duration::from_millis(10)).await,
                _ => panic!("unhandled error on CAS failure"),
            };
        }
    }
}

pub async fn run() {
    let (node, rx) = Node::new();
    let thunk_id_generator = ThunkIdGen::new();
    node.run(rx, thunk_id_generator).await;
}
