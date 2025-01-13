use crate::maelstrom;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

pub struct Node {
    pub id: Arc<Mutex<String>>,
    pub next_msg_id: AtomicUsize,
    pub neighbors: Arc<RwLock<Vec<String>>>,
    pub messages: Arc<Mutex<HashSet<serde_json::Value>>>,
    pub callbacks: Arc<Mutex<HashMap<usize, Box<dyn Fn(&maelstrom::Message) + Send>>>>,
    pub log_mutex: Mutex<()>,
}

impl Node {
    pub fn new(id: &str, neighbors: Vec<&str>) -> Self {
        Self {
            id: Arc::new(Mutex::new(String::from(id))),
            next_msg_id: AtomicUsize::from(0),
            neighbors: Arc::new(RwLock::new(
                neighbors.into_iter().map(String::from).collect(),
            )),
            messages: Default::default(),
            callbacks: Default::default(),
            log_mutex: Mutex::new(()),
        }
    }

    pub fn handle(&self, msg: &maelstrom::Message) {
        match &msg.body {
            maelstrom::Body::Init { msg_id, .. } => {
                {
                    let mut guard = self.id.lock().unwrap();
                    *guard = msg.dest.clone();
                    self.log(&format!("Initialized node {}", guard));
                }

                self.send(
                    &msg.src,
                    &maelstrom::Body::InitOk {
                        msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)), // TODO can this be more relaxed?
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            maelstrom::Body::InitOk { .. } => todo!(),
            maelstrom::Body::Echo { msg_id, echo, .. } => {
                self.send(
                    &msg.src,
                    &maelstrom::Body::EchoOk {
                        msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)), // TODO can this be more relaxed?
                        in_reply_to: msg_id.clone(),
                        echo: echo.clone(),
                    },
                );
            }
            maelstrom::Body::Topology { msg_id, topology } => {
                {
                    let neighbors = topology
                        .get(&self.id.lock().unwrap().to_string())
                        .unwrap_or(&vec![])
                        .clone();
                    let mut neighbors_guard = self.neighbors.write().unwrap();
                    *neighbors_guard = neighbors;
                    self.log(&format!("My neighbors are {:?}", neighbors_guard));
                }
                self.send(
                    &msg.src,
                    &maelstrom::Body::TopologyOk {
                        msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)), // TODO can this be more relaxed?
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            broadcast_body @ maelstrom::Body::Broadcast { msg_id, message } => {
                self.log(&format!("Received broadcast msg {:?}", broadcast_body));

                // avoid re-broadcasting messages already seen by this node.
                {
                    let mut messages_guard = self.messages.lock().unwrap();
                    if messages_guard.get(message).is_some() {
                        self.log(&format!(
                            "I've seen and re-broadcasted this msg before. Won't re-broadcast.",
                        ));
                        return;
                    }

                    messages_guard.insert(message.clone());
                }

                self.log(&format!(
                    "Re-broadcasting to my neighbors: {:?}",
                    self.neighbors
                ));

                let unacked = Arc::new(Mutex::new(HashSet::<String>::new()));

                {
                    let neighbors_guard = self.neighbors.read().unwrap();
                    neighbors_guard
                        .iter()
                        .filter(|&neighbor| *neighbor != msg.src)
                        .for_each(|neighbor| {
                            // TODO needs tested
                            // Register a callback to expect a broadcast_ok reply from this node
                            let unacked = unacked.clone();
                            {
                                unacked.lock().unwrap().insert(neighbor.clone());
                            }

                            let neighbor_copy = neighbor.clone();
                            {
                                self.callbacks.lock().unwrap().insert(
                                    msg_id.unwrap(),
                                    Box::new(move |response: &maelstrom::Message| {
                                        if matches!(
                                            response.body,
                                            maelstrom::Body::BroadcastOk { .. }
                                        ) {
                                            unacked.lock().unwrap().remove(&neighbor_copy);
                                        };
                                    }),
                                );
                            }

                            self.send(&neighbor, broadcast_body);
                            self.log(&format!("Re-broadcasted to neighbor: {:?}", neighbor));
                        });
                }

                {
                    let unacked_guard = unacked.lock().unwrap();
                    // Loop over unacked until other threads empty it for us by processing the registered callbacks.
                    while !unacked_guard.is_empty() {
                        thread::sleep(Duration::from_secs(1));
                    }
                }

                if let Some(msg_id) = msg_id {
                    self.send(
                        &msg.src,
                        &maelstrom::Body::BroadcastOk {
                            msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)), // TODO can this be more relaxed?
                            in_reply_to: msg_id.clone(),
                        },
                    );
                }
            }
            maelstrom::Body::BroadcastOk {
                msg_id,
                in_reply_to,
            } => {
                let mut callbacks_guard = self.callbacks.lock().unwrap();
                if let Some(callback) = callbacks_guard.get(in_reply_to) {
                    callback(msg);
                    callbacks_guard.remove(in_reply_to);
                    return;
                }
            }
            maelstrom::Body::Read { msg_id } => {
                self.send(
                    &msg.src,
                    &maelstrom::Body::ReadOk {
                        msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)), // TODO can this be more relaxed?
                        in_reply_to: msg_id.clone(),
                        messages: self.messages.lock().unwrap().clone(),
                    },
                );
            }
            _ => panic!("no matching handler. got msg: {:?}", msg),
        }
    }

    fn log(&self, log_msg: &str) {
        let _g = self.log_mutex.lock().unwrap();
        eprintln!("{}", log_msg);
    }

    fn send(&self, dest: &str, body: &maelstrom::Body) {
        let msg = maelstrom::Message {
            src: self.id.lock().unwrap().clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        println!("{}", serde_json::to_string(&msg).unwrap());
        self.next_msg_id.fetch_add(1, Ordering::SeqCst);
    }
}
