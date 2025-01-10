use std::collections::HashSet;

pub struct SimpleNode {
    pub id: String,
    pub next_msg_id: usize,
    pub neighbors: Vec<String>,
    pub messages: HashSet<String>,
}

impl SimpleNode {
    pub fn new() -> Self {
        Self {
            id: String::from(""),
            next_msg_id: 0,
            neighbors: Vec::new(),
            messages: HashSet::new(),
        }
    }

    pub fn broadcast(&mut self, msg: String) {
        for neighbor_index in 0..self.neighbors.len() {
            self.send(neighbor_index, &msg);
        }

        self.messages.insert(msg);
    }

    pub fn send(&mut self, neighbor_index: usize, msg: &String) {
        println!(
            "sending msg {msg} to neighbor {}",
            self.neighbors[neighbor_index]
        );
        self.next_msg_id += 1;
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
