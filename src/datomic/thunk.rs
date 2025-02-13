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

use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
};

use serde::{Deserialize, Serialize};

use crate::datomic::message::Body;

use super::transactor::Node;

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
pub(crate) enum ThunkValue {
    Intermediate(HashMap<usize, Thunk>),
    Ultimate(Vec<usize>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) struct Thunk {
    id: ThunkId,
    #[serde(skip_serializing)]
    state: ThunkState,
}

impl Thunk {
    pub fn new(thunk_id: ThunkId, value: ThunkValue) -> Self {
        Self {
            id: thunk_id,
            state: ThunkState::NotInStorage(value),
        }
    }

    pub async fn store(&mut self, node: &Node) {
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

    pub async fn value(&mut self, node: &Node) -> ThunkValue {
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

#[derive(Debug)]
pub(crate) struct ThunkIdGen {
    next_id: AtomicUsize,
}

impl ThunkIdGen {
    fn new(node_id: &str) -> Self {
        Self {
            next_id: AtomicUsize::new(0),
        }
    }

    pub fn generate(&self, node_id: &str) -> String {
        let id = format!("{}-{}", node_id, self.next_id.load(Ordering::SeqCst));
        self.next_id.fetch_add(1, Ordering::SeqCst);
        return id;
    }
}
