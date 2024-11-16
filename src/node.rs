use crate::maelstrom;
use std::collections::HashSet;
use std::sync::Mutex;

pub struct Node {
    pub id: String,
    pub next_msg_id: u8,
    pub neighbors: Vec<String>,
    messages: HashSet<serde_json::Value>,
    log_mutex: Mutex<()>,
}

impl Node {
    pub fn new(id: &str, neighbors: Vec<&str>) -> Self {
        Self {
            id: String::from(id),
            next_msg_id: 0,
            neighbors: neighbors.into_iter().map(String::from).collect(),
            messages: HashSet::new(),
            log_mutex: Mutex::new(()),
        }
    }

    pub fn handle(&mut self, msg: &maelstrom::Message) {
        match &msg.body {
            maelstrom::Body::Init(body) => {
                self.id = msg.dest.clone();
                self.log(&format!("Initialized node {}", self.id));

                self.send(
                    &msg.src,
                    &maelstrom::Body::InitOk(maelstrom::InitOkBody {
                        msg_id: Some(self.next_msg_id),
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
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: body.msg_id,
                        echo: body.echo.clone(),
                    }),
                );
            }
            maelstrom::Body::Topology(body) => {
                self.neighbors = body.topology.get(&self.id).unwrap_or(&vec![]).clone();
                self.log(&format!("My neighbors are {:?}", self.neighbors));
                self.send(
                    &msg.src,
                    &maelstrom::Body::TopologyOk(maelstrom::TopologyOkBody {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: body.msg_id,
                    }),
                );
            }
            broadcast_body @ maelstrom::Body::Broadcast(body) => {
                self.log(&format!("Received broadcast msg {:?}", broadcast_body));

                // avoid re-broadcasting messages already seen by this node.
                if self.messages.get(&body.message).is_some() {
                    self.log(&format!(
                        "I've seen and re-broadcasted this msg before. Wont re-broadcast.",
                    ));
                    return;
                }

                self.messages.insert(body.message.clone());

                let mut body_without_msg_id = body.clone();
                body_without_msg_id.msg_id = None;

                self.log(&format!(
                    "Re-broadcasting to my neighbors: {:?}",
                    self.neighbors
                ));

                // TODO avoid cloning here
                let neighbors = self.neighbors.clone();

                for node in neighbors {
                    self.send(&node, broadcast_body);
                    self.log(&format!("Re-broadcasted to neighbor: {:?}", node));
                }

                // TODO do we always return broadcast_ok?
                if let Some(msg_id) = body.msg_id {
                    self.send(
                        &msg.src,
                        &maelstrom::Body::BroadcastOk(maelstrom::BroadcastOkBody {
                            msg_id: Some(self.next_msg_id),
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
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: body.msg_id,
                        messages: self.messages.clone(),
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

    fn send(&mut self, dest: &str, body: &maelstrom::Body) {
        let msg = maelstrom::Message {
            src: self.id.clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        println!("{}", serde_json::to_string(&msg).unwrap());
        self.next_msg_id += 1;
    }
}
