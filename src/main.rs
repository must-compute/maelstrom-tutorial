mod maelstrom;
mod node;

use node::Node;
use std::{io, sync::Arc, thread};

fn main() {
    let node = Arc::new(Node::new());

    let mut input = String::new();
    let mut is_reading_stdin = true;
    while is_reading_stdin {
        // multi-producer, single consumer
        // consumer:  the retry thread
        //               consumes: NewMsg(Message) or RemoveUnacked(MessageID)
        // producer 1: handle broadcast : sends NewMsg
        // producer 2: handle broadcast_ok: sends remove unacked

        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let j: Result<maelstrom::Message, serde_json::Error> = serde_json::from_str(&input);
                let node = node.clone();

                thread::spawn(move || match j {
                    Ok(msg) => {
                        node.handle(&msg);
                    }
                    Err(err) => println!("json error: {:?}", err),
                });
            }
            Err(error) => {
                println!("readline error: {error}");
                is_reading_stdin = false;
            }
        }
        input.clear();
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use serde_json::json;
//     use std::collections::HashMap;

//     #[test]
//     fn test_maelstrom_message_deserialization() {
//         let echo_json = json!({
//             "src": "c1",
//             "dest": "n1",
//             "body": {
//                 "msg_id": 1,
//                 "type": "echo",
//                 "echo": "hello there"
//             }
//         });

//         let echo_message: maelstrom::Message = serde_json::from_value(echo_json).unwrap();
//         assert!(matches!(echo_message.body, maelstrom::Body::Echo(_)));

//         let init_json = json!({
//             "src": "c1",
//             "dest": "n1",
//             "body": {
//                 "type": "init",
//                 "msg_id": 1,
//                 "node_id": "n1",
//                 "node_ids": ["n1", "n2", "n3"]
//             }
//         });

//         let init_message: maelstrom::Message = serde_json::from_value(init_json).unwrap();
//         assert!(matches!(init_message.body, maelstrom::Body::Init(_)));
//     }

//     #[test]
//     fn test_responding_to_init_message() {
//         let init_msg_id = 1;
//         let init_msg = maelstrom::Message {
//             src: String::from("c1"),
//             dest: String::from("n1"),
//             body: maelstrom::Body::Init(maelstrom::InitBody {
//                 msg_id: init_msg_id,
//                 msg_type: String::from("init"),
//                 node_id: String::from("n1"),
//                 node_ids: vec![String::from("n1"), String::from("n2"), String::from("n3")],
//             }),
//         };
//         let mut node = Node::new("", vec![]);
//         let starting_next_msg_id = node.next_msg_id;
//         let response = node.handle(&init_msg);

//         assert_eq!(node.id, init_msg.dest);
//         assert_eq!(response.src, init_msg.dest);
//         assert_eq!(response.dest, init_msg.src);

//         if let maelstrom::Body::InitOk(maelstrom::InitOkBody {
//             msg_id,
//             msg_type,
//             in_reply_to,
//         }) = response.body
//         {
//             assert_eq!(msg_id, starting_next_msg_id + 1);
//             assert_eq!(msg_type, "init_ok");
//             assert_eq!(in_reply_to, init_msg_id);
//         } else {
//             panic!("unexpected body in init msg test")
//         }
//     }

//     #[test]
//     fn test_responding_to_echo_message() {
//         let echo_msg_id = 1;
//         let msg_body = serde_json::to_value("hello").unwrap();
//         let echo_msg = maelstrom::Message {
//             src: String::from("c1"),
//             dest: String::from("n1"),
//             body: maelstrom::Body::Echo(maelstrom::EchoBody {
//                 msg_id: echo_msg_id,
//                 msg_type: String::from("echo"),
//                 echo: msg_body.clone(),
//             }),
//         };
//         let mut node = Node::new(&echo_msg.dest, vec![]);
//         let starting_next_msg_id = node.next_msg_id;
//         let response = node.handle(&echo_msg);

//         assert_eq!(response.src, echo_msg.dest);
//         assert_eq!(response.dest, echo_msg.src);

//         if let maelstrom::Body::EchoOk(maelstrom::EchoOkBody {
//             msg_id,
//             msg_type,
//             in_reply_to,
//             echo,
//         }) = response.body
//         {
//             assert_eq!(msg_id, starting_next_msg_id + 1);
//             assert_eq!(msg_type, "echo_ok");
//             assert_eq!(in_reply_to, echo_msg_id);
//             assert_eq!(echo, msg_body);
//         } else {
//             panic!("unexpected body in echo msg test")
//         }
//     }

//     #[test]
//     fn test_responding_to_topology_message() {
//         let topology_msg_id = 1;
//         let mut topology: HashMap<String, Vec<String>> = HashMap::new();
//         let node_id = String::from("n1");
//         topology.insert(node_id.clone(), vec![String::from("n2")]);
//         let topology_msg = maelstrom::Message {
//             src: String::from("c1"),
//             dest: node_id.clone(),
//             body: maelstrom::Body::Topology(maelstrom::TopologyBody {
//                 msg_id: topology_msg_id,
//                 msg_type: String::from("topology"),
//                 topology: topology.clone(),
//             }),
//         };
//         let mut node = Node::new(&topology_msg.dest, vec![]);
//         let starting_next_msg_id = node.next_msg_id;
//         let response = node.handle(&topology_msg);

//         assert_eq!(response.src, topology_msg.dest);
//         assert_eq!(response.dest, topology_msg.src);
//         assert_eq!(node.neighbors, topology.get(&node_id).unwrap().clone());

//         if let maelstrom::Body::TopologyOk(maelstrom::TopologyOkBody {
//             msg_id,
//             msg_type,
//             in_reply_to,
//         }) = response.body
//         {
//             assert_eq!(msg_id, starting_next_msg_id + 1);
//             assert_eq!(msg_type, "topology_ok");
//             assert_eq!(in_reply_to, topology_msg_id);
//         } else {
//             panic!("unexpected body in topology msg test")
//         }
//     }
// }
