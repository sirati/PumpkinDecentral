use crate::realm_completion::PeerGeneration;
use crate::time::TickStamp;

pub struct FrozenActorTickFrame<T> {
    pub source: PeerGeneration,
    pub tick: TickStamp,
    jobs: Vec<T>,
    cursor: usize,
}

impl<T> FrozenActorTickFrame<T> {
    pub fn new(source: PeerGeneration, tick: TickStamp, jobs: Vec<T>) -> Option<Self> {
        (source.generation != 0).then_some(Self {
            source,
            tick,
            jobs,
            cursor: 0,
        })
    }

    pub fn remaining(&self) -> &[T] {
        &self.jobs[self.cursor..]
    }

    pub fn remaining_mut(&mut self) -> &mut [T] {
        &mut self.jobs[self.cursor..]
    }

    pub fn commit_emitted(&mut self, count: usize) -> bool {
        if count > self.jobs.len() - self.cursor {
            return false;
        }
        self.cursor += count;
        true
    }

    pub fn complete(&self) -> bool {
        self.cursor == self.jobs.len()
    }
}
