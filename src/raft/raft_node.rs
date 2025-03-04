use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

use super::{kv_store::KeyValueStore, log::Log, raft::NodeState};

pub type StateMachineKey = usize;
pub type StateMachineValue = usize;

#[derive(Debug)]
pub(super) struct RaftNode {
    pub current_term: AtomicUsize,
    pub log: Mutex<Log>,
    pub my_id: Mutex<String>,               // TODO use oncelock
    pub other_node_ids: Mutex<Vec<String>>, // TODO use oncelock
    pub node_state: Mutex<NodeState>,
    pub voted_for: Mutex<Option<String>>,
    pub state_machine: Mutex<KeyValueStore<StateMachineKey, StateMachineValue>>, // TODO use dashmap?
    pub next_msg_id: AtomicUsize,
    pub commit_index: AtomicUsize,
    pub next_index: Mutex<HashMap<String, usize>>,
    pub match_index: Mutex<HashMap<String, usize>>,
    pub last_applied: AtomicUsize,
}

impl Default for RaftNode {
    fn default() -> Self {
        Self {
            current_term: Default::default(),
            log: Mutex::new(Log::new()),
            my_id: Default::default(),
            node_state: Mutex::new(NodeState::FollowerOf(None)),
            other_node_ids: Default::default(),
            voted_for: Mutex::new(None),
            state_machine: Default::default(),
            next_msg_id: Default::default(),
            commit_index: Default::default(),
            next_index: Mutex::new(HashMap::new()),
            match_index: Mutex::new(HashMap::new()),
            last_applied: AtomicUsize::new(1),
        }
    }
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

    pub fn median_match_index_value(&self) -> usize {
        let mut indices = self.match_index().into_values().collect::<Vec<usize>>();
        indices.sort();
        let median = indices.len() - self.majority_count();
        indices[median]
    }

    pub fn reserve_next_msg_id(&self) -> usize {
        self.next_msg_id.fetch_add(1, Ordering::SeqCst)
    }

    pub fn match_index(&self) -> HashMap<String, usize> {
        let mut match_index = self.match_index.lock().unwrap();
        match_index.insert(
            self.my_id.lock().unwrap().clone(),
            self.log.lock().unwrap().len(),
        );
        match_index.clone()
    }
}
