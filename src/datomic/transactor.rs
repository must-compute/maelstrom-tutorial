use std::{collections::HashMap, sync::Arc};

use crate::datomic::message::{Body, ErrorCode};
use anyhow::Result;
use tokio::sync::mpsc::{Receiver, Sender};

use super::message::{Message, Transaction};

type ResponseNotifier = tokio::sync::oneshot::Sender<Message>;

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

// events that handle() will generate
pub enum Event {
    Send {
        message: Message,
        notifier: Option<ResponseNotifier>,
    },
    Received {
        response: Message,
    },
    Init {
        id: String,
        node_ids: Vec<String>,
    },
    GetNodeId {
        tx: tokio::sync::oneshot::Sender<String>,
    },
}

pub struct Node {
    event_tx: Sender<Event>,
    // pub id: String,
    // pub node_ids: Vec<String>, // Every node in the network (including this node)
}

impl Node {
    fn new() -> (Self, Receiver<Event>) {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel::<Event>(32);
        (

            Self {
                event_tx
            },
            event_rx
        )
    }

    async fn run(mut self, mut event_rx: Receiver<Event>) {
        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);


        // event handler task
        tokio::spawn(async move {
            let mut unacked: HashMap<usize, ResponseNotifier> = Default::default();
            let mut node_id: String = Default::default();
            let mut ids: Vec<String> = Default::default();
            let mut next_msg_id = 0;

            while let Some(event) = event_rx.recv().await {
                match event {
                    Event::Init { id, node_ids } => {
                        node_id = id;
                        ids = node_ids;
                    }
                    Event::GetNodeId { tx } => tx.send(node_id.clone()).unwrap(),
                    Event::Send {
                        mut message,
                        notifier,
                    } => {
                        if !matches!(message.body, Body::TxnOk { .. })
                            && !matches!(message.body, Body::Error { .. })
                        {
                            message.body.set_msg_id(next_msg_id);
                            next_msg_id += 1;
                        }
                        println!("{}", serde_json::to_string(&message).unwrap());
                        if let Some(notifier) = notifier {
                            unacked.insert(message.body.msg_id(), notifier);
                        }
                    }
                    Event::Received { response } => {
                        // we received an ack, so we notify and remove from unacked.
                        if let Some(notifier) = unacked.remove(&response.body.in_reply_to()) {
                            notifier.send(response).unwrap();
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
                eprintln!("received json msg: {:?}", json_msg);
                stdin_tx.send(json_msg).await.unwrap();
                input.clear();
            }
        });

        let node = Arc::new(self);
        loop {
            tokio::select! {
                Some(json_msg) = stdin_rx.recv() => {
                    tokio::spawn({
                        let node = node.clone();
                        async move {node.handle(&json_msg ).await}
                    });
                }
                //...
            }
        }
    }

    // TODO this can now be a free function (now that Node is useless)
    async fn handle(&self, msg: &Message) {
        match &msg.body {
            Body::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                self.event_tx.send(Event::Init {
                    id: node_id.clone(),
                    node_ids: node_ids.clone(),
                })
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
                let txn_result = self.transact(txn.clone()).await;

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
                self.event_tx.send(Event::Received {
                    response: msg.clone(),
                })
                .await
                .unwrap();
            }
        }
    }

    pub async fn send(
        &self,
        notifier: Option<ResponseNotifier>,
        dest: &str,
        body: &Body,
    ) {
        let (id_tx, id_rx) = tokio::sync::oneshot::channel::<String>();

        self.event_tx.send(Event::GetNodeId { tx: id_tx }).await.unwrap();

        let message = Message {
            src: id_rx.await.unwrap(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        self.event_tx
            .send(Event::Send { message, notifier })
            .await
            .unwrap();
    }

    // blocks until obtaining the response Message
    pub async fn sync_rpc(&self, dest: &str, msg_body: &Body) -> Message {
        let (notifier_tx, notifier_rx) = tokio::sync::oneshot::channel::<Message>();
        self.send(Some(notifier_tx), dest, msg_body)
            .await;
        let response = notifier_rx.await.unwrap();
        response
    }

    pub async fn transact(
        &self,
        mut txn: Transaction,
    ) -> Result<Transaction> {
        let root = String::from("ROOT");
        let lin_kv = String::from("lin-kv");

        let read_msg_body = Body::Read {
            msg_id: 0,
            key: root.clone(),
        };

        let response = self
            .sync_rpc(&lin_kv, &read_msg_body)
            .await;

        let root_pointer: Option<???> = match response.body {
            Body::ReadOk { value, .. } => {
                let value = serde_json::from_value(value)
                    .expect("lin-kv ReadOk should return a ThunkValue");
                Some(value)
            }
            Body::Error { code, .. } => {
                if code == ErrorCode::KeyDoesNotExist {
                    todo!()
                } else {
                    panic!("unexpected error code while reading from lin-kv");
                }
            }
            _ => panic!("something went wrong while reading from lin-kv"),
        };

        let mut local_snapshot = initial_read.clone();

        for op in txn.iter_mut() {
            match op {
                MicroOperation::Read { key, value } => {
                    *value = local_snapshot.get(key).map(|v| v.to_owned())
                }
                MicroOperation::Append { key, value } => {
                    local_snapshot.entry(*key).or_insert(vec![]).push(*value)
                }
            }
        }

        // cas
        let cas_msg_body = Body::Cas {
            msg_id: 0,
            key: root.clone(),
            from: serde_json::to_value(initial_read).unwrap(),
            to: serde_json::to_value(local_snapshot.clone())?,
            create_if_not_exists: true,
        };

        let response = self
            .sync_rpc(&lin_kv, &cas_msg_body)
            .await;

        if matches!(
            response.body,
            Body::Error {
                code: ErrorCode::PreconditionFailed,
                ..
            }
        ) {
            let err = anyhow::Error::new(DatomicError::TransactionConflict);
            return Err(err);
        }

        Ok(txn)
    }
}

fn state_from_json_value(value: serde_json::Value) -> ThunkValue {
    value
        .as_object()
        .expect("value should be a JSON object")
        .iter()
        .map(|(k, v)| {
            (
                k.parse::<usize>().expect("key should be a number"),
                v.as_array()
                    .expect("value should be an array")
                    .iter()
                    .map(|n| n.as_u64().expect("array element should be a number") as usize)
                    .collect(),
            )
        })
        .collect()
}

pub async fn run() {
    let (node, rx) = Node::new();
    node.run(rx).await;
}
