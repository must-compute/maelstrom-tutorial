use crate::maelstrom;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

#[derive(Default)]
pub struct Node {
    pub id: RwLock<String>,
    pub next_msg_id: AtomicUsize,
    pub neighbors: RwLock<Vec<String>>,
    messages: Mutex<HashSet<serde_json::Value>>,
    callbacks: Mutex<HashMap<usize, Box<dyn Fn(maelstrom::Message) + Send + 'static>>>,
}

impl Node {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn handle(&self, msg: &maelstrom::Message) {
        // TODO this is temporary
        let log_prefix = format!("WHILE PROCESSING: {:?}, logged:\t", msg);

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
                    Self::log(&format!("My neighbors are {:?}", *neighbors_guard));
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
                Self::log(&format!("{} Received broadcast msg {:?}", log_prefix, msg));
                if let Some(msg_id) = msg_id {
                    Self::log(&format!("{log_prefix} Sending BroadcastOk to client"));
                    self.send(
                        &msg.src,
                        &maelstrom::Body::BroadcastOk {
                            msg_id: Some(self.next_msg_id.load(Ordering::SeqCst)),
                            in_reply_to: msg_id.clone(),
                        },
                    );
                }
                {
                    Self::log(&format!(
                        "{} Current messages: {:?}",
                        log_prefix,
                        self.messages.lock().unwrap(),
                    ));
                }

                let mut messages_guard = self.messages.lock().unwrap();

                // avoid re-broadcasting messages already seen by this node.
                if messages_guard.get(message).is_none() {
                    messages_guard.insert(message.clone());
                    Self::log(&format!(
                        "{} Update messages: {:?}",
                        log_prefix, messages_guard
                    ));
                    drop(messages_guard);

                    // self.log(&format!(
                    //     "Re-broadcasting to my neighbors: {:?}",
                    //     self.neighbors
                    // ));

                    let unacked = Arc::new(Mutex::new(Vec::<String>::new()));
                    {
                        let mut unacked_guard = unacked.lock().unwrap();
                        self.neighbors
                            .read()
                            .unwrap()
                            .iter()
                            .filter(|&neighbor| {
                                let cond = *neighbor != msg.src;
                                if cond {
                                    Self::log(&format!(
                                        "{log_prefix} filter: keeping {neighbor} in unacked {:?}",
                                        unacked
                                    ));
                                }
                                cond
                            })
                            .for_each(|neighbor| {
                                unacked_guard.push(neighbor.clone());
                                // self.send(neighbor, broadcast_body);
                                // self.log(&format!("Re-broadcasted to neighbor: {:?}", neighbor));
                            });
                        Self::log(&format!(
                            "{log_prefix} Initial unacked: {:?}",
                            unacked_guard
                        ));
                    }

                    let cloned_1 = unacked.clone();

                    let mut count = 0;
                    loop {
                        Self::log(&format!("{log_prefix} counter: {count}"));
                        count += 1;
                        {
                            let unacked_guard = cloned_1.lock().unwrap();
                            Self::log(&format!(
                                "{log_prefix} Unacked: empty? {}, {:?}",
                                unacked_guard.is_empty(),
                                unacked_guard
                            ));
                            if unacked_guard.is_empty() {
                                Self::log("{log_prefix} Unacked is empty, stop looping");
                                break;
                            }
                            unacked_guard.iter().for_each(|dest| {
                                let dest = dest.clone();

                                let cloned_2 = unacked.clone();
                                let msg = msg.clone();
                                self.rpc(&log_prefix, dest.clone(), broadcast_body,  move |response| {
                                    Self::log(&format!(
                                        "WITHIN RPC: this RPC was spawned by {:?} and it received response {:?}",
                                        msg,
                                        response
                                    ));
                                    if matches!(response.body, maelstrom::Body::BroadcastOk { .. })
                                    {
                                        let mut vec = cloned_2.lock().unwrap();
                                        Self::log(&format!(
                                            "WITHIN RPC: this RPC was spawned by {:?}, and it is removing {} from current unacked {:?}",
                                            msg, dest, vec
                                        ));
                                        vec.retain(|node_name| *node_name != dest);
                                        Self::log(&format!(
                                            "WITHIN RPC: this RPC was spawned by {:?}, and here is Unacked after removing {}: {:?}",
                                            msg, dest, vec
                                        ));
                                    }
                                });
                            });
                        }
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            }
            maelstrom::Body::BroadcastOk { in_reply_to, .. } => {
                let mut callbacks_guard = self.callbacks.lock().unwrap();
                Self::log("Received BroadcastOk");
                if let Some(cb) = callbacks_guard.get(in_reply_to) {
                    Self::log(&format!(
                        "{log_prefix} Executing callback for {}",
                        in_reply_to
                    ));
                    cb(msg.clone());
                    callbacks_guard.remove(in_reply_to);
                };
            }
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

    fn send(&self, dest: &str, body: &maelstrom::Body) {
        Self::log(&format!("entering send"));
        let msg_id = self.next_msg_id.load(Ordering::SeqCst);
        let mut body = body.clone();
        body.set_msg_id(msg_id);

        let msg = maelstrom::Message {
            src: self.id.read().unwrap().clone(),
            dest: dest.to_string(),
            body,
        };

        println!("{}", serde_json::to_string(&msg).unwrap());
        Self::log(&format!("printed"));
        self.next_msg_id.fetch_add(1, Ordering::SeqCst);
        Self::log(&format!("incremented next_msg_id"));
    }

    fn rpc(
        &self,
        log_prefix: &str,
        dest: String,
        body: &maelstrom::Body,
        cb: (impl Fn(maelstrom::Message) + Send + 'static),
    ) {
        Self::log(&format!("{log_prefix} Sending RPC call to {}", dest));
        {
            self.callbacks
                .lock()
                .unwrap()
                .insert(self.next_msg_id.load(Ordering::SeqCst), Box::new(cb));
        }
        Self::log(&format!("{log_prefix} after inserting callbacks"));
        self.send(&dest, body);
        Self::log(&format!("{log_prefix} after sending"));
    }

    fn log(s: &str) {
        eprintln!("{}", s);
        // {
        //     let mut guard = std::io::stderr().lock();
        //     guard
        //         .write_all(format!("aaaaaaaaaaaaaa {}\n", s).as_bytes())
        //         .unwrap();
        // }
    }
}
