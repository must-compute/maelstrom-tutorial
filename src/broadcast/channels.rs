use crate::maelstrom;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use std::{io, thread};

enum RetryMessage {
    Retry(maelstrom::Message),
    StopRetry(usize), // TODO use a custom msg id type
}

#[derive(Default)]
pub struct Node {
    pub id: RwLock<String>,
    pub next_msg_id: AtomicUsize,
    pub neighbors: RwLock<Vec<String>>,
    messages: Mutex<HashSet<serde_json::Value>>,
    retry_tx: OnceLock<mpsc::Sender<RetryMessage>>,
}

impl Node {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn handle(self: Arc<Self>, msg: &maelstrom::Message) {
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

                //////
                let (retry_tx, retry_rx) = mpsc::channel();
                self.retry_tx.get_or_init(|| retry_tx);

                thread::scope(move |s| {
                    // thread::spawn(move || {
                    let unacked: Arc<Mutex<Vec<maelstrom::Message>>> =
                        Arc::new(Mutex::new(Vec::new()));

                    s.spawn({
                        // thread::spawn({
                        let unacked = unacked.clone();
                        move || loop {
                            match retry_rx.recv() {
                                Ok(RetryMessage::Retry(msg)) => unacked.lock().unwrap().push(msg),
                                Ok(RetryMessage::StopRetry(msg_id)) => unacked
                                    .lock()
                                    .unwrap()
                                    .retain(|msg| msg.body.msg_id() != msg_id),
                                Err(_) => todo!(),
                            }
                        }
                    });

                    s.spawn({
                        // thread::spawn({
                        let unacked = unacked.clone();
                        move || loop {
                            unacked.lock().unwrap().iter().for_each(|msg| {
                                self.send_reserved(msg);
                            });
                            thread::sleep(Duration::from_secs(1));
                        }
                    });
                });
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
            maelstrom::Body::Broadcast { msg_id, message } => {
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

                // avoid re-broadcasting messages already seen by this node.
                let mut message_is_new = false;
                let mut messages_guard = self.messages.lock().unwrap();
                if messages_guard.get(message).is_none() {
                    messages_guard.insert(message.clone());
                    Self::log(&format!(
                        "{} Update messages: {:?}",
                        log_prefix, messages_guard
                    ));
                    drop(messages_guard);

                    message_is_new = true;
                }

                if message_is_new {
                    let new_broadcast_src = self.id.read().unwrap();
                    self.neighbors
                        .read()
                        .unwrap()
                        .iter()
                        .filter(|&neighbor| *neighbor != msg.src)
                        .for_each(|neighbor| {
                            let mut neighbor_broadcast_msg = msg.clone();
                            neighbor_broadcast_msg.src = new_broadcast_src.clone();
                            neighbor_broadcast_msg.dest = neighbor.clone();
                            let msg_id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
                            neighbor_broadcast_msg.body.set_msg_id(msg_id);

                            self.retry_tx
                                .get()
                                .unwrap()
                                .send(RetryMessage::Retry(neighbor_broadcast_msg))
                                .unwrap();
                        });
                }
            }
            maelstrom::Body::BroadcastOk { in_reply_to, .. } => {
                self.retry_tx
                    .get()
                    .unwrap()
                    .send(RetryMessage::StopRetry(*in_reply_to))
                    .unwrap();
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
        Self::log(&format!("entering send for {dest} and msg body {:?}", body));
        let msg_id = self.next_msg_id.load(Ordering::SeqCst);
        let mut body = body.clone();
        body.set_msg_id(msg_id);

        let msg = maelstrom::Message {
            src: self.id.read().unwrap().clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        println!("{}", serde_json::to_string(&msg).unwrap());
        Self::log(&format!("printed send for {dest} and msg body {:?}", body));
        self.next_msg_id.fetch_add(1, Ordering::SeqCst);
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

pub fn run() {
    let node = Arc::new(Node::new());

    let mut input = String::new();
    let mut is_reading_stdin = true;

    while is_reading_stdin {
        if let Err(e) = io::stdin().read_line(&mut input) {
            println!("readline error: {e}");
            is_reading_stdin = false;
        }

        let json_msg = serde_json::from_str(&input).expect("should take a JSON message");

        thread::spawn({
            let node = node.clone();
            move || {
                node.handle(&json_msg);
            }
        });

        input.clear();
    }
}
