//      ┌──────────┐
// ROOT:│   n0-3   │
//      └────┬─────┘
//           ▼
//         ┌────┐
//       9:│n0-2┼─► [1,2]
//         └────┘
//         ┌────┐
//       6:│n0-0┼─► [1]
//         └────┘

use core::hash;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::datomic::message::Body;

use super::transactor::{Event, Node};

type ThunkId = String;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum ValueState {
    NotEvaluated,
    Evaluated(ThunkValue),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum ThunkState {
    InStorage(ValueState),
    NotInStorage(ThunkValue),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
enum ThunkValue {
    Intermediate(HashMap<usize, Thunk>),
    Ultimate(Vec<usize>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Thunk {
    id: ThunkId,
    #[serde(skip_serializing)]
    state: ThunkState,
}

impl Thunk {
    fn new(thunk_id: ThunkId, value: ThunkValue) -> Self {
        Self {
            id: thunk_id,
            state: ThunkState::NotInStorage(value),
        }
    }

    async fn store(&mut self, node: &Node) {
        match self.state {
            ThunkState::InStorage(_) => (),
            ThunkState::NotInStorage(ref mut thunk_value) => {
                if let ThunkValue::Intermediate(ref mut hash_map) = thunk_value {
                    for (_, v) in hash_map.iter_mut() {
                        v.store(node);
                    }
                };

                let response = node
                    .sync_rpc(
                        "lin-kv",
                        &Body::Write {
                            msg_id: 1,
                            key: self.id.clone(),
                            value: serde_json::to_value(thunk_value.clone()).unwrap(),
                        },
                    )
                    .await;

                if !matches!(response.body, Body::WriteOk { .. }) {
                    panic!("we couldnt write");
                }

                self.state = ThunkState::InStorage(ValueState::Evaluated(thunk_value.clone()));
            }
        }
    }

    async fn value(&mut self, node: &Node) -> ThunkValue {
        match &self.state {
            ThunkState::NotInStorage(thunk_value)
            | ThunkState::InStorage(ValueState::Evaluated(thunk_value)) => thunk_value.clone(),

            ThunkState::InStorage(ValueState::NotEvaluated) => {
                let response = node
                    .sync_rpc(
                        "lin-kv",
                        &Body::Read {
                            msg_id: 1,
                            key: self.id.clone(),
                        },
                    )
                    .await;

                if let Body::ReadOk { value, .. } = response.body {
                    let thunk_value: ThunkValue = serde_json::from_value(value).unwrap();
                    self.state = ThunkState::InStorage(ValueState::Evaluated(thunk_value.clone()));
                    return thunk_value;
                } else {
                    panic!("we couldnt read");
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ThunkIdGen {
    node_id: String,
    next_id: usize,
}

impl ThunkIdGen {
    fn new(node_id: &str) -> Self {
        Self {
            node_id: node_id.to_string(),
            next_id: 0,
        }
    }

    fn generate(&mut self) -> String {
        let id = format!("{}-{}", self.node_id, self.next_id);
        self.next_id += 1;
        return id;
    }
}
