use serde::{Deserialize, Serialize};
use std::{collections::HashSet, io};

use crate::maelstrom_generic::{self, MessageBody};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum CustomBody {
    Replicate { msg_id: usize },
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
    pub next_msg_id: usize,
    pub neighbors: Vec<String>,
    pub messages: HashSet<serde_json::Value>,
}

impl Node {
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn run(&mut self) {
        let mut input = String::new();
        let mut is_reading_stdin = true;
        while is_reading_stdin {
            if let Err(e) = io::stdin().read_line(&mut input) {
                println!("readline error: {e}");
                is_reading_stdin = false;
            }

            let json_msg = serde_json::from_str(&input).expect("should take a JSON message");
            eprintln!("received json msg: {:?}", json_msg);
            self.handle(&json_msg).await;

            input.clear();
        }
    }

    pub async fn handle(&mut self, msg: &maelstrom_generic::Message<CustomBody>) {
        match &msg.body {
            maelstrom_generic::Body::Init { msg_id, .. } => {
                self.id = msg.dest.clone();

                self.send(
                    &msg.src,
                    &maelstrom_generic::Body::InitOk {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            maelstrom_generic::Body::Topology { msg_id, topology } => {
                self.neighbors = topology
                    .get(&self.id.to_string())
                    .unwrap_or(&vec![])
                    .clone();

                self.send(
                    &msg.src,
                    &maelstrom_generic::Body::TopologyOk {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            maelstrom_generic::Body::GSetAdd { msg_id, element } => {
                self.messages.insert(element.clone());
                self.send(
                    &msg.src,
                    &maelstrom_generic::Body::GSetAddOk {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            maelstrom_generic::Body::Read { msg_id } => {
                self.send(
                    &msg.src,
                    &maelstrom_generic::Body::GSetReadOk {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: msg_id.clone(),
                        value: self.messages.clone(),
                    },
                );
            }
            maelstrom_generic::Body::Custom(CustomBody::Replicate { msg_id }) => {
                todo!()
            }
            _ => todo!(),
        }
    }

    fn send(&self, dest: &str, body: &maelstrom_generic::Body<CustomBody>) {
        let msg_id = self.next_msg_id;
        let mut body = body.clone();
        body.set_msg_id(msg_id);

        let msg = maelstrom_generic::Message {
            src: self.id.clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        eprintln!("MSG DEBUG {:?}", msg);
        println!("{}", serde_json::to_string(&msg).unwrap());
        self.next_msg_id;
    }
}

pub async fn run() {
    let mut node = Node::new();
    node.run().await;
}
