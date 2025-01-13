use crate::maelstrom;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock};

#[derive(Default)]
pub struct Node {
    pub id: RwLock<String>,
    pub next_msg_id: AtomicUsize,
    pub neighbors: RwLock<Vec<String>>,
    messages: Mutex<HashSet<serde_json::Value>>,
    // log_mutex: Mutex<()>,
}

impl Node {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn handle(&self, msg: &maelstrom::Message) {
        match &msg.body {
            maelstrom::Body::Init { msg_id, .. } => {
                {
                    let mut id_guard = self.id.write().unwrap();
                    *id_guard = msg.dest.clone();
                }
                // self.log(&format!("Initialized node {}", self.id));

                self.send(
                    &msg.src,
                    &maelstrom::Body::InitOk {
                        msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)), // TODO investigate weaker ordering
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            maelstrom::Body::InitOk { .. } => todo!(),
            maelstrom::Body::Echo { msg_id, echo, .. } => {
                self.send(
                    &msg.src,
                    &maelstrom::Body::EchoOk {
                        msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)), // TODO investigate weaker ordering
                        in_reply_to: msg_id.clone(),
                        echo: echo.clone(),
                    },
                );
            }
            maelstrom::Body::Topology { msg_id, topology } => {
                let neighbors = topology
                    .get(&self.id.read().unwrap().to_string())
                    .unwrap_or(&vec![])
                    .clone();

                {
                    let mut neighbors_guard = self.neighbors.write().unwrap();
                    *neighbors_guard = neighbors;
                    // self.log(&format!("My neighbors are {:?}", self.neighbors));
                }

                self.send(
                    &msg.src,
                    &maelstrom::Body::TopologyOk {
                        msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)), // TODO investigate weaker ordering
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            broadcast_body @ maelstrom::Body::Broadcast { msg_id, message } => {
                // self.log(&format!("Received broadcast msg {:?}", broadcast_body));

                {
                    let mut messages_guard = self.messages.lock().unwrap();
                    // avoid re-broadcasting messages already seen by this node.
                    if messages_guard.get(message).is_some() {
                        // self.log(&format!(
                        //     "I've seen and re-broadcasted this msg before. Wont re-broadcast.",
                        // ));
                        return;
                    }

                    messages_guard.insert(message.clone());
                }

                // self.log(&format!(
                //     "Re-broadcasting to my neighbors: {:?}",
                //     self.neighbors
                // ));

                {
                    self.neighbors
                        .read()
                        .unwrap()
                        .iter()
                        .filter(|&neighbor| *neighbor != msg.src)
                        .for_each(|neighbor| {
                            self.send(neighbor, broadcast_body);
                            // self.log(&format!("Re-broadcasted to neighbor: {:?}", neighbor));
                        });
                }

                if let Some(msg_id) = msg_id {
                    self.send(
                        &msg.src,
                        &maelstrom::Body::BroadcastOk {
                            msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)),
                            in_reply_to: msg_id.clone(),
                        },
                    );
                }
            }
            maelstrom::Body::BroadcastOk { .. } => {}
            maelstrom::Body::Read { msg_id } => {
                self.send(
                    &msg.src,
                    &maelstrom::Body::ReadOk {
                        msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)),
                        in_reply_to: msg_id.clone(),
                        messages: self.messages.lock().unwrap().clone(),
                    },
                );
            }
            _ => panic!("no matching handler. got msg: {:?}", msg),
        }
    }

    // fn log(&self, log_msg: &str) {
    //     let _g = self.log_mutex.lock().unwrap();
    //     eprintln!("{}", log_msg);
    // }

    fn send(&self, dest: &str, body: &maelstrom::Body) {
        let msg = maelstrom::Message {
            src: self.id.read().unwrap().clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        println!("{}", serde_json::to_string(&msg).unwrap());
        self.next_msg_id.fetch_add(1, Ordering::SeqCst);
    }
}
