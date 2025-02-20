use std::collections::HashMap;

use tokio::sync::mpsc::Sender;

use super::{
    event::{Command, Event, Query},
    message::{Body, ErrorCode, Message},
};

struct KeyValueStore {
    map: HashMap<String, String>,
}

impl KeyValueStore {
    pub fn read(&self, key: &str) -> Option<String> {
        self.map.get(key).map(|s| s.to_owned())
    }

    pub fn write(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    pub fn cas(&mut self, key: String, from: String, to: String) {
        let res = self.map.get_mut(&key);

        match res {
            Some(current) => {
                if *current != from {
                    panic!("from != current");
                }
                *current = to
            }
            None => panic!("key does not exist"),
        }
    }
}

pub async fn run() {
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel::<Message>(32);
    let (event_tx, mut event_rx) = tokio::sync::mpsc::channel::<Event>(32);

    // event handler task
    tokio::spawn(async move {
        let mut unacked: HashMap<usize, tokio::sync::oneshot::Sender<Message>> = Default::default();
        let mut node_id: String = Default::default();
        let mut _ids: Vec<String> = Default::default();
        let mut next_msg_id = 0;

        while let Some(event) = event_rx.recv().await {
            match event {
                Event::Cast(Command::Init { id, node_ids }) => {
                    node_id = id;
                    _ids = node_ids;
                }
                Event::Call(Query::GetNodeId { responder }) => responder
                    .send(node_id.clone())
                    .expect("respond to Query::GetNodeId"),
                Event::Call(Query::SendViaMaelstrom { ref message, .. })
                | Event::Cast(Command::SendViaMaelstrom { ref message }) => {
                    let mut message = message.clone();
                    println!(
                        "{}",
                        serde_json::to_string(&message)
                            .expect("msg being sent to STDOUT should be serializable to JSON")
                    );

                    tracing::debug!("sent msg {:?}", &message);

                    match event {
                        Event::Call(Query::SendViaMaelstrom { responder, .. }) => {
                            // TODO i dont like this at all, because     ^message
                            //      will have an old msg id if captured, due to shadowing
                            let msg_id = message.body.msg_id();
                            unacked.insert(msg_id, responder);
                        }
                        _ => (),
                    };
                }
                Event::Cast(Command::ReceivedViaMaelstrom { response }) => {
                    // we received an ack, so we notify and remove from unacked.
                    if let Some(notifier) = unacked.remove(&response.body.in_reply_to()) {
                        notifier
                            .send(response)
                            .expect("returning msg ack should work over the oneshot channel");
                    }
                }
                Event::Call(Query::ReserveMsgId { responder: tx }) => {
                    tx.send(next_msg_id).unwrap();
                    next_msg_id += 1;
                }
                Event::Call(Query::KVRead { key, responder }) => {}
                Event::Cast(Command::KVWrite { key, value }) => {}
                Event::Call(Query::KVCas {
                    key,
                    from,
                    to,
                    responder,
                }) => {}
            }
        }
    });

    // stdin task
    tokio::spawn(async move {
        let mut input = String::new();
        let mut is_reading_stdin = true;
        while is_reading_stdin {
            if let Err(e) = std::io::stdin().read_line(&mut input) {
                println!("readline error: {e}");
                is_reading_stdin = false;
            }

            let json_msg = serde_json::from_str(&input)
                .expect(&format!("should take a JSON message. Got {:?}", input));
            tracing::debug!("received json msg: {:?}", json_msg);
            stdin_tx.send(json_msg).await.unwrap();
            input.clear();
        }
    });

    loop {
        tokio::select! {
            Some(json_msg) = stdin_rx.recv() => {
                tokio::spawn({
                    let event_tx = event_tx.clone();
                    async move {handle(event_tx, json_msg).await}
                });
            }
            //...
        }
    }
}

async fn handle(event_tx: Sender<Event>, msg: Message) -> () {
    match msg.body {
        Body::Init {
            node_id, node_ids, ..
        } => {
            event_tx
                .send(Event::Cast(Command::Init {
                    id: node_id,
                    node_ids,
                }))
                .await
                .expect("should be able to send Init event");
        }
        Body::Read { msg_id, key } => {
            let (tx, rx) = tokio::sync::oneshot::channel::<Option<String>>();
            event_tx
                .send(Event::Call(Query::KVRead { key, responder: tx }))
                .await
                .expect("should be able to send Read event");
            let response = rx
                .await
                .expect("should be able to read from Read oneshot channel");

            send(
                event_tx.clone(),
                None,
                msg.src,
                match response {
                    Some(value) => Body::ReadOk {
                        msg_id: Some(reserve_msg_id(event_tx).await),
                        in_reply_to: msg_id,
                        value: serde_json::to_value(&value)
                            .expect("value should be serializable to json"),
                    },
                    None => Body::Error {
                        in_reply_to: msg_id,
                        code: ErrorCode::KeyDoesNotExist,
                        text: "key does not exist".to_string(),
                    },
                },
            )
            .await
        }
        Body::Write { msg_id, key, value } => todo!(),
        Body::Cas {
            msg_id,
            key,
            from,
            to,
        } => todo!(),

        Body::InitOk { .. }
        | Body::ReadOk { .. }
        | Body::WriteOk { .. }
        | Body::CasOk { .. }
        | Body::Error { .. } => panic!(),
    }
    ()
}

pub async fn reserve_msg_id(event_tx: Sender<Event>) -> usize {
    let (tx, rx) = tokio::sync::oneshot::channel::<usize>();
    event_tx
        .send(Event::Call(Query::ReserveMsgId { responder: tx }))
        .await
        .unwrap();
    rx.await.unwrap()
}

pub async fn send(
    event_tx: Sender<Event>,
    responder: Option<tokio::sync::oneshot::Sender<Message>>,
    dest: String,
    body: Body,
) {
    let (id_tx, id_rx) = tokio::sync::oneshot::channel::<String>();

    event_tx
        .send(Event::Call(Query::GetNodeId { responder: id_tx }))
        .await
        .unwrap();

    let message = Message {
        src: id_rx.await.unwrap(),
        dest: dest.to_string(),
        body: body.clone(),
    };

    let event = if let Some(responder) = responder {
        Event::Call(Query::SendViaMaelstrom { message, responder })
    } else {
        Event::Cast(Command::SendViaMaelstrom { message })
    };
    event_tx.send(event).await.unwrap();
}
