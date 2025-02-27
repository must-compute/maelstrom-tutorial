use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use super::{kv_store::KeyValueStore, log::Log, raft::NodeState};

pub type StateMachineKey = usize;
pub type StateMachineValue = usize;

#[derive(Debug)]
pub struct RaftNode {
    pub current_term: AtomicUsize,
    pub log: Mutex<Log>,
    pub my_id: Mutex<String>,               // TODO use oncelock
    pub other_node_ids: Mutex<Vec<String>>, // TODO use oncelock
    pub node_state: Mutex<NodeState>,
    pub voted_for: Mutex<Option<String>>,
    pub state_machine: Mutex<KeyValueStore<StateMachineKey, StateMachineValue>>, // TODO use dashmap?
}

impl RaftNode {
    pub fn advance_term_to(&self, new_term: usize) {
        let _ = self.current_term.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            |mut current_term| {
                assert!(new_term > current_term);
                current_term = new_term;
                Some(current_term)
            },
        );
        *self.voted_for.lock().unwrap() = None;
    }

    pub(crate) fn set_node_state(&mut self, new_state: NodeState) {
        *self.node_state.lock().unwrap() = new_state;
    }

    pub fn majority_count(&self) -> usize {
        let all_nodes_count = self.other_node_ids.lock().unwrap().len() + 1;
        (all_nodes_count / 2) + 1
    }
}
