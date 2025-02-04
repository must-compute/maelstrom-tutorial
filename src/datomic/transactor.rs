use crate::datomic::message::Body;
use tokio::sync::mpsc::Sender;

use super::message::Message;

#[derive(Default)]
pub struct Node {
    pub id: String,
    pub node_ids: Vec<String>, // Every node in the network (including this node)
}

impl Node {
    fn new() -> Self {
        Self::default()
    }

    async fn run(mut self) {
        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel::<Message>(32);

        let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);

        //write task
        tokio::spawn(async move {
            let mut next_msg_id = 0;
            while let Some(mut msg) = msg_rx.recv().await {
                if !matches!(msg.body, Body::TxnOk { .. }) {
                    msg.body.set_msg_id(next_msg_id);
                    next_msg_id += 1;
                }
                println!("{}", serde_json::to_string(&msg).unwrap());
            }
        });

        tokio::spawn(async move {
            let mut input = String::new();
            let mut is_reading_stdin = true;
            while is_reading_stdin {
                if let Err(e) = std::io::stdin().read_line(&mut input) {
                    println!("readline error: {e}");
                    is_reading_stdin = false;
                }

                let json_msg = serde_json::from_str(&input).expect("should take a JSON message");
                eprintln!("received json msg: {:?}", json_msg);
                stdin_tx.send(json_msg).await.unwrap();
                input.clear();
            }
        });

        loop {
            tokio::select! {
                Some(json_msg) = stdin_rx.recv() => self.handle(&json_msg, msg_tx.clone()).await,
            }
        }
    }

    async fn handle(&mut self, msg: &Message, tx: Sender<Message>) {
        eprintln!("{:?}", msg);
        match &msg.body {
            Body::Init {
                msg_id,
                node_id,
                node_ids,
            } => {
                self.id = node_id.clone();
                self.node_ids = node_ids.clone();

                self.send(
                    tx,
                    &msg.src,
                    &Body::InitOk {
                        msg_id: Some(0),
                        in_reply_to: msg_id.clone(),
                    },
                )
                .await;
            }
            Body::Topology { msg_id, topology } => todo!(),
            Body::Txn { msg_id, txn } => {
                self.send(
                    tx,
                    &msg.src,
                    &Body::TxnOk {
                        txn: txn.clone(),
                        in_reply_to: msg.body.msg_id(),
                    },
                )
                .await
            }
            _ => todo!(),
        }
    }

    async fn send(&self, msg_tx: Sender<Message>, dest: &str, body: &Body) {
        let msg = Message {
            src: self.id.clone(),
            dest: dest.to_string(),
            body: body.clone(),
        };

        msg_tx.send(msg).await.unwrap();
    }
}

pub async fn run() {
    Node::new().run().await;
}
