use serde::{Deserialize, Serialize};
use std::{collections::HashSet, io, time::Duration};
use tokio::sync::mpsc::Sender;

use crate::maelstrom_generic::{self, MessageBody};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum CustomBody {
    Replicate {
        msg_id: usize,
        value: HashSet<serde_json::Value>,
    },
}

impl MessageBody for CustomBody {
    fn msg_id(&self) -> usize {
        match self {
            CustomBody::Replicate { msg_id, .. } => *msg_id,
        }
    }

    fn set_msg_id(&mut self, new_id: usize) {
        match self {
            CustomBody::Replicate { ref mut msg_id, .. } => *msg_id = new_id,
        };
    }
}

#[derive(Default)]
pub struct Node {
    pub id: String,
    pub node_ids: Vec<String>, // Every node in the network (including this node)
    pub neighbors: Vec<String>,
    pub messages: HashSet<serde_json::Value>,
}

impl Node {
    pub fn new() -> Self {
        Default::default()
    }

    async fn replicate(&self, msg_tx: Sender<maelstrom_generic::Message<CustomBody>>) {
        eprintln!("REPLICATING CURRENT SET: {:?}", self.neighbors.clone());

        for node in self.node_ids.iter() {
            if *node != self.id {
                let body = maelstrom_generic::Body::Custom(CustomBody::Replicate {
                    msg_id: 0, // TODO this is ugly. Use a constructor
                    value: self.messages.clone(),
                });

                let msg = maelstrom_generic::Message::<CustomBody> {
                    src: self.id.clone(),
                    dest: node.clone(),
                    body,
                };

                msg_tx.send(msg).await.unwrap();
            }
        }
    }

    pub async fn run(mut self) {
        let (msg_tx, mut msg_rx) =
            tokio::sync::mpsc::channel::<maelstrom_generic::Message<CustomBody>>(32);

        let (stdin_tx, mut stdin_rx) =
            tokio::sync::mpsc::channel::<maelstrom_generic::Message<CustomBody>>(32);

        tokio::spawn(async move {
            let mut next_msg_id = 0;
            while let Some(mut msg) = msg_rx.recv().await {
                msg.body.set_msg_id(next_msg_id);
                next_msg_id += 1;
                println!("{}", serde_json::to_string(&msg).unwrap());
            }
        });

        tokio::spawn(async move {
            let mut input = String::new();
            let mut is_reading_stdin = true;
            while is_reading_stdin {
                if let Err(e) = io::stdin().read_line(&mut input) {
                    println!("readline error: {e}");
                    is_reading_stdin = false;
                }

                let json_msg = serde_json::from_str(&input).expect("should take a JSON message");
                eprintln!("received json msg: {:?}", json_msg);
                stdin_tx.send(json_msg).await.unwrap();
                input.clear();
            }
        });

        let mut interval = tokio::time::interval(Duration::from_secs(3));
        loop {
            tokio::select! {
                Some(json_msg) = stdin_rx.recv() => self.handle(&json_msg, msg_tx.clone()).await,
                _ = interval.tick() => self.replicate(msg_tx.clone()).await,
            }
        }
    }

    pub async fn handle(
        &mut self,
        msg: &maelstrom_generic::Message<CustomBody>,
        tx: Sender<maelstrom_generic::Message<CustomBody>>,
    ) {
        match &msg.body {
            maelstrom_generic::Body::Init {
                msg_id, node_ids, ..
            } => {
                self.id = msg.dest.clone();
                self.node_ids = node_ids.clone();

                self.send(
                    tx,
                    &msg.src,
                    &maelstrom_generic::Body::InitOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                    },
                )
                .await;
            }
            maelstrom_generic::Body::Topology { msg_id, topology } => {
                self.neighbors = topology
                    .get(&self.id.to_string())
                    .unwrap_or(&vec![])
                    .clone();

                self.send(
                    tx,
                    &msg.src,
                    &maelstrom_generic::Body::TopologyOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                    },
                )
                .await;
            }
            maelstrom_generic::Body::GSetAdd { msg_id, element } => {
                self.messages.insert(element.clone());
                self.send(
                    tx,
                    &msg.src,
                    &maelstrom_generic::Body::GSetAddOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                    },
                )
                .await;
            }
            maelstrom_generic::Body::Read { msg_id } => {
                self.send(
                    tx,
                    &msg.src,
                    &maelstrom_generic::Body::GSetReadOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                        value: self.messages.clone(),
                    },
                )
                .await;
            }
            maelstrom_generic::Body::Custom(CustomBody::Replicate { msg_id, value }) => {
                self.messages = self.messages.union(value).cloned().collect();
            }
            _ => todo!(),
        }
    }

    async fn send(
        &self,
        msg_tx: Sender<maelstrom_generic::Message<CustomBody>>,
        dest: &str,
        body: &maelstrom_generic::Body<CustomBody>,
    ) {
        let msg = maelstrom_generic::Message {
            src: self.id.clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        msg_tx.send(msg).await.unwrap();
    }
}

pub async fn run() {
    let mut node = Node::new();
    node.run().await;
}
