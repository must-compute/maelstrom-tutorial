use crate::maelstrom;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::sync::Mutex;

pub struct Node {
    pub id: RefCell<String>,
    pub next_msg_id: Cell<u8>,
    pub neighbors: RefCell<Vec<String>>,
    messages: RefCell<HashSet<serde_json::Value>>,
    log_mutex: Mutex<()>,
}

impl Node {
    pub fn new(id: &str, neighbors: Vec<&str>) -> Self {
        Self {
            id: RefCell::new(String::from(id)),
            next_msg_id: Cell::new(0),
            neighbors: RefCell::new(neighbors.into_iter().map(String::from).collect()),
            messages: RefCell::new(HashSet::new()),
            log_mutex: Mutex::new(()),
        }
    }

    pub fn handle(&self, msg: &maelstrom::Message) {
        match &msg.body {
            maelstrom::Body::Init(body) => {
                *self.id.borrow_mut() = msg.dest.clone();
                self.log(&format!("Initialized node {}", self.id.borrow()));

                self.send(
                    &msg.src,
                    &maelstrom::Body::InitOk(maelstrom::InitOkBody {
                        msg_id: Some(self.next_msg_id.get()),
                        in_reply_to: body.msg_id,
                    }),
                );
            }
            maelstrom::Body::InitOk(_body) => todo!(),
            maelstrom::Body::Echo(body) => {
                self.log(&format!("Echoing {:?}", body));

                self.send(
                    &msg.src,
                    &maelstrom::Body::EchoOk(maelstrom::EchoOkBody {
                        msg_id: Some(self.next_msg_id.get()),
                        in_reply_to: body.msg_id,
                        echo: body.echo.clone(),
                    }),
                );
            }
            maelstrom::Body::Topology(body) => {
                let neighbors = body
                    .topology
                    .get(&self.id.borrow().to_string())
                    .unwrap_or(&vec![])
                    .clone();
                *self.neighbors.borrow_mut() = neighbors;
                self.log(&format!("My neighbors are {:?}", self.neighbors));
                self.send(
                    &msg.src,
                    &maelstrom::Body::TopologyOk(maelstrom::TopologyOkBody {
                        msg_id: Some(self.next_msg_id.get()),
                        in_reply_to: body.msg_id,
                    }),
                );
            }
            broadcast_body @ maelstrom::Body::Broadcast(body) => {
                self.log(&format!("Received broadcast msg {:?}", broadcast_body));

                // avoid re-broadcasting messages already seen by this node.
                if self.messages.borrow().get(&body.message).is_some() {
                    self.log(&format!(
                        "I've seen and re-broadcasted this msg before. Wont re-broadcast.",
                    ));
                    return;
                }

                self.messages.borrow_mut().insert(body.message.clone());

                let mut body_without_msg_id = body.clone();
                body_without_msg_id.msg_id = None;

                self.log(&format!(
                    "Re-broadcasting to my neighbors: {:?}",
                    self.neighbors
                ));

                for node in self.neighbors.borrow().iter() {
                    self.send(&node, broadcast_body);
                    self.log(&format!("Re-broadcasted to neighbor: {:?}", node));
                }

                // TODO do we always return broadcast_ok?
                if let Some(msg_id) = body.msg_id {
                    self.send(
                        &msg.src,
                        &maelstrom::Body::BroadcastOk(maelstrom::BroadcastOkBody {
                            msg_id: Some(self.next_msg_id.get()),
                            in_reply_to: msg_id,
                        }),
                    );
                }
            }
            maelstrom::Body::BroadcastOk(_body) => {}
            maelstrom::Body::Read(body) => {
                self.send(
                    &msg.src,
                    &maelstrom::Body::ReadOk(maelstrom::ReadOkBody {
                        msg_id: Some(self.next_msg_id.get()),
                        in_reply_to: body.msg_id,
                        messages: self.messages.borrow().clone(),
                    }),
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
            src: self.id.borrow().clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        println!("{}", serde_json::to_string(&msg).unwrap());
        let updated_next_msg_id = self.next_msg_id.get() + 1;
        self.next_msg_id.set(updated_next_msg_id);
    }
}
