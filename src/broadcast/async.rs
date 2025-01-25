use crate::maelstrom;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{io, thread};
use tokio::sync::mpsc::{self, Sender};

enum RetryMessage {
    Retry(maelstrom::Message),
    StopRetry(usize), // TODO use a custom msg id type
}

#[derive(Default)]
pub struct Node {
    pub id: String,
    pub next_msg_id: usize,
    pub neighbors: Vec<String>,
    messages: HashSet<serde_json::Value>,
}

impl Node {
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn handle(&mut self, msg: &maelstrom::Message, tx: Sender<RetryMessage>) {
        // TODO this is temporary
        let log_prefix = format!("WHILE PROCESSING: {:?}, logged:\t", msg);

        match &msg.body {
            maelstrom::Body::Init { msg_id, .. } => {
                self.id = msg.dest.clone();

                self.send(
                    &msg.src,
                    &maelstrom::Body::InitOk {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            maelstrom::Body::InitOk { .. } => todo!(),
            maelstrom::Body::Echo { msg_id, echo, .. } => {
                self.send(
                    &msg.src,
                    &maelstrom::Body::EchoOk {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: msg_id.clone(),
                        echo: echo.clone(),
                    },
                );
            }
            maelstrom::Body::Topology { msg_id, topology } => {
                let neighbors = topology
                    .get(&self.id.to_string())
                    .unwrap_or(&vec![])
                    .clone();

                self.neighbors = neighbors;

                Self::log(&format!("My neighbors are {:?}", self.neighbors));

                self.send(
                    &msg.src,
                    &maelstrom::Body::TopologyOk {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            maelstrom::Body::Broadcast { msg_id, message } => {
                Self::log(&format!("{} Received broadcast msg {:?}", log_prefix, msg));

                if let Some(msg_id) = msg_id {
                    Self::log(&format!("{log_prefix} Sending BroadcastOk to client"));
                    self.send(
                        &msg.src,
                        &maelstrom::Body::BroadcastOk {
                            msg_id: Some(self.next_msg_id),
                            in_reply_to: msg_id.clone(),
                        },
                    );
                }

                {
                    Self::log(&format!(
                        "{} Current messages: {:?}",
                        log_prefix, self.messages,
                    ));
                }

                // avoid re-broadcasting messages already seen by this node.
                let mut message_is_new = false;
                if self.messages.get(message).is_none() {
                    self.messages.insert(message.clone());
                    Self::log(&format!(
                        "{} Update messages: {:?}",
                        log_prefix, self.messages
                    ));

                    message_is_new = true;
                }

                if message_is_new {
                    let new_broadcast_src = self.id.clone();
                    let filtered_neighbors = self
                        .neighbors
                        .iter()
                        .filter(|&neighbor| *neighbor != msg.src)
                        .cloned()
                        .collect::<Vec<String>>();

                    for neighbor in filtered_neighbors {
                        let mut neighbor_broadcast_msg = msg.clone();
                        neighbor_broadcast_msg.src = new_broadcast_src.clone();
                        neighbor_broadcast_msg.dest = neighbor.clone();
                        let msg_id = self.next_msg_id;
                        self.next_msg_id += 1;
                        neighbor_broadcast_msg.body.set_msg_id(msg_id);
                        tx.send(RetryMessage::Retry(neighbor_broadcast_msg))
                            .await
                            .unwrap();
                    }
                }
            }
            maelstrom::Body::BroadcastOk { in_reply_to, .. } => {
                tx.send(RetryMessage::StopRetry(*in_reply_to))
                    .await
                    .unwrap();
            }
            maelstrom::Body::Read { msg_id } => {
                self.send(
                    &msg.src,
                    &maelstrom::Body::ReadOk {
                        msg_id: Some(self.next_msg_id),
                        in_reply_to: msg_id.clone(),
                        messages: self.messages.clone(),
                    },
                );
            }
            _ => panic!("no matching handler. got msg: {:?}", msg),
        }
    }

    fn send(&self, dest: &str, body: &maelstrom::Body) {
        Self::log(&format!("entering send for {dest} and msg body {:?}", body));
        let msg_id = self.next_msg_id;
        let mut body = body.clone();
        body.set_msg_id(msg_id);

        let msg = maelstrom::Message {
            src: self.id.clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        println!("{}", serde_json::to_string(&msg).unwrap());
        Self::log(&format!("printed send for {dest} and msg body {:?}", body));
        self.next_msg_id;
        Self::log(&format!(
            "incremented next msg id send for {dest} and msg body {:?}",
            body
        ));
    }

    // assumes the id in the message was reserved by using then incrementing self.next_msg_id upon message construction.
    fn send_reserved(&self, message: &maelstrom::Message) {
        println!("{}", serde_json::to_string(message).unwrap());
    }

    fn log(s: &str) {
        eprintln!("{} | THREAD ID: {:?}", s, thread::current().id());
    }
}

pub async fn run() {
    let mut node = Node::new();

    let mut input = String::new();
    let mut is_reading_stdin = true;

    let (tx, mut rx) = mpsc::channel(32);
    let unacked: Arc<Mutex<Vec<maelstrom::Message>>> = Arc::new(Mutex::new(Vec::new()));

    tokio::spawn({
        let unacked = unacked.clone();
        async move {
            while let Some(message) = rx.recv().await {
                match message {
                    RetryMessage::Retry(msg) => unacked.lock().unwrap().push(msg),
                    RetryMessage::StopRetry(msg_id) => unacked
                        .lock()
                        .unwrap()
                        .retain(|msg| msg.body.msg_id() != msg_id),
                }
            }
        }
    });

    let unacked = unacked.clone();
    tokio::spawn(async move {
        loop {
            unacked.lock().unwrap().iter().for_each(|msg| {
                println!("{}", serde_json::to_string(msg).unwrap());
            });
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    while is_reading_stdin {
        if let Err(e) = io::stdin().read_line(&mut input) {
            println!("readline error: {e}");
            is_reading_stdin = false;
        }

        let json_msg = serde_json::from_str(&input).expect("should take a JSON message");
        node.handle(&json_msg, tx.clone()).await;

        input.clear();
    }
}
