use super::{log::Log, raft::NodeState};

#[derive(Debug, Clone)]
pub struct RaftNode {
    pub current_term: usize,
    pub log: Log,
    pub my_id: String,
    pub node_state: NodeState,
    pub other_node_ids: Vec<String>,
    pub voted_for: Option<String>,
}

impl RaftNode {
    pub fn become_follower_of(&mut self, leader: &str) {
        self.node_state = NodeState::FollowerOf(leader.to_string());
    }

    pub fn set_voted_for(&mut self, candidate: &str) {
        self.voted_for = Some(candidate.to_string());
    }

    pub fn advance_term_to(&mut self, new_term: usize) {
        assert!(new_term > self.current_term);
        self.current_term = new_term;
        self.voted_for = None;
    }

    pub(crate) fn set_node_state(&mut self, new_state: NodeState) {
        self.node_state = new_state;
    }

    pub fn majority_count(&self) -> usize {
        let all_nodes_count = self.other_node_ids.len() + 1;
        (all_nodes_count / 2) + 1
    }
}
