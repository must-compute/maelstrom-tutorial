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

use std::collections::HashMap;

use super::transactor::Node;

type ThunkId = String;

#[derive(Clone, Debug, PartialEq)]
enum ValueState {
    NotEvaluated,
    Evaluated(ThunkValue),
}

#[derive(Clone, Debug, PartialEq)]
enum ThunkState {
    InStorage(ValueState),
    NotInStorage(ThunkValue),
}

#[derive(Clone, Debug, PartialEq)]
enum ThunkValue {
    Intermediate(HashMap<usize, Thunk>),
    Ultimate(Vec<usize>),
}

#[derive(Clone, Debug, PartialEq)]
struct Thunk {
    id: ThunkId,
    state: ThunkState,
}

impl Thunk {
    fn new(thunk_id: ThunkId, value: ThunkValue) -> Self {
        Self {
            id: thunk_id,
            state: ThunkState::NotInStorage(value),
        }
    }

    fn store(&mut self, node: &Node) {
        match self.state {
            ThunkState::InStorage(value_state) => (),
            ThunkState::NotInStorage(thunk_value) => {
                // lin-kv write
                // await write_ok
                // self.state = in storage
                todo!()
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
