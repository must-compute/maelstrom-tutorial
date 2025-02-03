use serde::{Deserialize, Serialize};
use std::{cmp::max, collections::HashMap, io, time::Duration};
use tokio::sync::mpsc::Sender;

use crate::maelstrom_generic::{self, MessageBody};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum CustomBody {
    Replicate {
        msg_id: usize,
        value: HashMap<String, usize>,
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
    pub counters: HashMap<String, usize>, // key: node name, value: counter value
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
                    value: self.counters.clone(),
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

                eprintln!("received raw msg: {:?}", input);

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
            maelstrom_generic::Body::GCounterAdd { msg_id, delta } => {
                // self.counters
                //     .entry(msg.src.clone())
                //     .and_modify(|v| *v += *delta)
                //     .or_insert(*delta);

                if let Some(existing) = self.counters.get_mut(&msg.src) {
                    *existing += *delta;
                } else {
                    self.counters.insert(msg.src.clone(), *delta);
                }

                self.send(
                    tx,
                    &msg.src,
                    &maelstrom_generic::Body::GCounterAddOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                    },
                )
                .await;
            }
            maelstrom_generic::Body::Read { msg_id } => {
                let sum = self.counters.values().sum();

                self.send(
                    tx,
                    &msg.src,
                    &maelstrom_generic::Body::GCounterReadOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                        value: sum,
                    },
                )
                .await;
            }
            maelstrom_generic::Body::Custom(CustomBody::Replicate { value, .. }) => {
                for (k, v) in self.counters.iter_mut() {
                    if let Some(other_v) = value.get(k) {
                        *v = max(*v, *other_v);
                    }
                }
                for (k, v) in value {
                    if self.counters.get(k).is_none() {
                        self.counters.insert(k.clone(), v.clone());
                    }
                }
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
    let node = Node::new();
    node.run().await;
}
