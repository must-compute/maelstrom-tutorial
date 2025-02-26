use super::message::Message;

#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub term: usize,
    pub op: Option<Message>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Log {
    entries: Vec<Entry>,
}

impl Log {
    pub fn new() -> Self {
        Self {
            entries: vec![Entry { term: 0, op: None }],
        }
    }

    pub fn get(&self, index: usize) -> Option<&Entry> {
        self.entries.get(index - 1)
    }

    pub fn append(&mut self, entries: &mut Vec<Entry>) {
        self.entries.append(entries);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn last_term(&self) -> usize {
        self.entries
            .last()
            .expect("append-only Log should never be empty")
            .term
    }
}
