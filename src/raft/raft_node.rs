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
    pub log: Arc<Mutex<Log>>,
    pub my_id: Arc<Mutex<String>>,               // TODO use oncelock
    pub other_node_ids: Arc<Mutex<Vec<String>>>, // TODO use oncelock
    pub node_state: Arc<Mutex<NodeState>>,
    pub voted_for: Arc<Mutex<Option<String>>>,
    pub state_machine: Arc<Mutex<KeyValueStore<StateMachineKey, StateMachineValue>>>, // TODO use dashmap?
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
