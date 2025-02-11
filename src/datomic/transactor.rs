use crate::datomic::message::{Body, ErrorCode};
use anyhow::Result;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::Sender;

use super::{
    message::{Message, Transaction},
    micro_op::MicroOperation,
};

type ResponseNotifier = tokio::sync::oneshot::Sender<Message>;

#[derive(Debug)]
enum DatomicError {
    TransactionFailed,
}

impl std::fmt::Display for DatomicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatomicError::TransactionFailed => write!(f, "Transaction failed"),
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

#[derive(Default)]
pub struct Node {
    // pub id: String,
    // pub node_ids: Vec<String>, // Every node in the network (including this node)
}

impl Node {
    fn new() -> Self {
        Self::default()
    }

    async fn run(self) {
        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Event>(32);

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
                        if !matches!(message.body, Body::TxnOk { .. }) {
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
                        let event_tx = event_tx.clone();
                        async move {node.handle(event_tx, &json_msg ).await}
                    });
                }
                //...
            }
        }
    }

    // TODO this can now be a free function (now that Node is useless)
    async fn handle(&self, tx: Sender<Event>, msg: &Message) {
        match &msg.body {
            Body::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                tx.send(Event::Init {
                    id: node_id.clone(),
                    node_ids: node_ids.clone(),
                })
                .await
                .unwrap();

                self.send(
                    tx,
                    None,
                    &msg.src,
                    &Body::InitOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                    },
                )
                .await;
            }
            Body::Topology { .. } => todo!(),
            Body::Txn { txn, .. } => {
                let txn_result = self.transact(tx.clone(), txn.clone()).await;
                self.send(
                    tx.clone(),
                    None, // TODO not sure
                    &msg.src,
                    &Body::TxnOk {
                        txn: txn_result.unwrap(),
                        in_reply_to: msg.body.msg_id(),
                    },
                )
                .await
            }
            _ => {
                tx.send(Event::Received {
                    response: msg.clone(),
                })
                .await
                .unwrap();
            }
        }
    }

    async fn send(
        &self,
        event_tx: Sender<Event>,
        notifier: Option<ResponseNotifier>,
        dest: &str,
        body: &Body,
    ) {
        let (id_tx, id_rx) = tokio::sync::oneshot::channel::<String>();

        event_tx.send(Event::GetNodeId { tx: id_tx }).await.unwrap();

        let message = Message {
            src: id_rx.await.unwrap(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        event_tx
            .send(Event::Send { message, notifier })
            .await
            .unwrap();
    }

    // blocks until obtaining the response Message
    async fn sync_rpc(&self, event_tx: Sender<Event>, dest: &str, msg_body: &Body) -> Message {
        let (notifier_tx, notifier_rx) = tokio::sync::oneshot::channel::<Message>();
        self.send(event_tx.clone(), Some(notifier_tx), dest, msg_body)
            .await;
        let response = notifier_rx.await.unwrap();
        response
    }

    pub async fn transact(
        &self,
        event_tx: Sender<Event>,
        mut txn: Transaction,
    ) -> Result<Transaction> {
        let root = String::from("ROOT");
        let lin_kv = String::from("lin-kv");

        let read_msg_body = Body::Read {
            msg_id: 0,
            key: root.clone(),
        };

        let response = self
            .sync_rpc(event_tx.clone(), &lin_kv, &read_msg_body)
            .await;

        let initial_read = match response.body {
            Body::ReadOk { value, .. } => state_from_json_value(value),
            Body::Error { code, .. } => {
                // https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors
                if code == ErrorCode::KeyDoesNotExist {
                    HashMap::<usize, Vec<usize>>::new()
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
            .sync_rpc(event_tx.clone(), &lin_kv, &cas_msg_body)
            .await;

        // TODO this is probably not needed
        if matches!(response.body, Body::Error { .. }) {
            // self.send(event_tx.clone(), None, dest, body).await
            // return Err(Box::new(Foo::A));
            return Err(DatomicError::TransactionFailed);
        }

        Ok(txn)
    }
}

fn state_from_json_value(value: serde_json::Value) -> HashMap<usize, Vec<usize>> {
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
    Node::new().run().await;
}
