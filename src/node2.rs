use std::collections::HashSet;

use crate::maelstrom;

#[derive(Default)]
pub struct SimpleNode {
    pub id: String,
    pub next_msg_id: usize,
    pub neighbors: Vec<String>,
    pub messages: HashSet<String>,
}

impl SimpleNode {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn broadcast(&mut self, msg: String) {
        for neighbor_index in 0..self.neighbors.len() {
            self.send(neighbor_index, &msg);
        }

        self.messages.insert(msg);
    }

    pub fn send_to_neighbor(&mut self, neighbor_index: usize, msg: &maelstrom::Message) {
        println!(
            "sending msg {msg} to neighbor {}",
            self.neighbors[neighbor_index]
        );
        self.next_msg_id += 1;
    }

    pub fn send(&mut self, dest: &str, msg: &maelstrom::Body) {
        let msg = maelstrom::Message {
            src: self.id.clone(),
            dest: dest.to_string(),
            body: msg.clone(),
        };
        println!("{}", serde_json::to_string(&msg).unwrap());
        self.next_msg_id += 1;
    }

    fn log(&self, log_msg: &str) {
        // let _g = self.log_mutex.lock().unwrap();
        eprintln!("{}", log_msg);
    }

    pub fn handle(&mut self, msg: &maelstrom::Message) {
        match &msg.body {
            maelstrom::Body::Init { msg_id, .. } => {
                self.id = msg.dest.clone();
                self.log(&format!("Initialized node {}", self.id));

                self.send(
                    &msg.src,
                    &maelstrom::Body::InitOk {
                        msg_id: Some(self.next_msg_id.get()),
                        in_reply_to: msg_id.clone(),
                    },
                );
            }
            maelstrom::Body::InitOk {
                msg_id,
                in_reply_to,
            } => todo!(),
            maelstrom::Body::Echo { msg_id, echo } => todo!(),
            maelstrom::Body::EchoOk {
                msg_id,
                in_reply_to,
                echo,
            } => todo!(),
            maelstrom::Body::Topology { msg_id, topology } => todo!(),
            maelstrom::Body::TopologyOk {
                msg_id,
                in_reply_to,
            } => todo!(),
            maelstrom::Body::Broadcast { msg_id, message } => todo!(),
            maelstrom::Body::BroadcastOk {
                msg_id,
                in_reply_to,
            } => todo!(),
            maelstrom::Body::Read { msg_id } => todo!(),
            maelstrom::Body::ReadOk {
                msg_id,
                in_reply_to,
                messages,
            } => todo!(),
            maelstrom::Body::Todo(value) => todo!(),
        }
    }
}

// impl test
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node2() {
        let mut my_node = SimpleNode {
            id: String::from("n1"),
            next_msg_id: 1,
            neighbors: vec![String::from("n2"), String::from("n3")],
            messages: HashSet::new(),
        };

        let my_node2 = SimpleNode::new();

        my_node.broadcast(String::from("hello world"));

        assert_eq!(my_node.next_msg_id, 3);
    }
}
