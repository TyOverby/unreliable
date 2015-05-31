use std::collections::{VecMap, HashMap};

#[derive(RustcEncodable, RustcDecodable, Clone, Copy)]
#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct MsgId(pub u64);

#[derive(RustcEncodable, RustcDecodable, Clone, Copy)]
#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct PieceNum(pub u16, pub u16);

#[derive(RustcEncodable, RustcDecodable, Clone)]
#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct MsgChunk(pub MsgId, pub PieceNum, pub Vec<u8>);

#[derive(RustcEncodable, RustcDecodable, Clone)]
#[derive(Hash, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct CompleteMessage(pub MsgId, pub Vec<u8>);

struct MsgStage {
    this_id: MsgId,
    total_pieces: u16,
    pieces: HashMap<usize, MsgChunk>,
    size: usize
}

pub struct MsgQueue {
    last_released: Option<MsgId>,
    stages: HashMap<MsgId, MsgStage>,
    max_size: Option<usize>,
    cur_size: usize
}

impl MsgQueue {
    pub fn new(max_size: Option<usize>) -> MsgQueue {
        MsgQueue {
            last_released: None,
            stages: HashMap::new(),
            max_size: max_size,
            cur_size: 0,
        }
    }

    // Sets an id as being published, in turn removing all
    // earlier messages.
    fn mark_published(&mut self, just_published: MsgId) {
        self.last_released = Some(just_published);
        let keys: Vec<_> = self.stages.keys().cloned().collect();
        for open in keys {
            if just_published > open {
                if let Some(stage) = self.stages.remove(&open) {
                    self.cur_size -= stage.size;
                }
            }
        }
    }

    // If we are over capacity, this function will remove messages from
    // the beginning of the queue until we are no longer above capacity.
    fn prune(&mut self) {
        if self.max_size.is_none() { return; }
        let max_size = self.max_size.unwrap();
        let mut open: Vec<_> = self.stages.keys().cloned().collect();
        (&mut open[..]).sort_by(|&MsgId(a), &MsgId(b)| b.cmp(&a));

        while self.cur_size > max_size {
            if let Some(id) = open.pop() {
                if let Some(stage) = self.stages.remove(&id) {
                    self.cur_size -= stage.size;
                }
            } else {
                break;
            }
        }
    }

    pub fn insert_chunk(&mut self, chunk: MsgChunk) -> Option<CompleteMessage> {
        let id = chunk.0;
        self.prune();

        // If the last published message was released before this chunk,
        // don't do anything and ignore it.
        if let Some(last) = self.last_released {
            if last.0 >= id.0 {
                return None;
            }
        }

        // If the chunk has only one piece to it, publish it immediately.
        if (chunk.1).1 == 1 {
            self.mark_published(id);
            return Some(CompleteMessage(id, chunk.2));
        }

        // If we are building a stage with the an existing message id, add it
        // to the stage.
        if self.stages.contains_key(&id) {
            let ready = {
                let stage = self.stages.get_mut(&id).unwrap();
                self.cur_size += stage.add_chunk(chunk);
                stage.is_ready()
            };

            if ready {
                let mut stage = self.stages.remove(&id).unwrap();
                self.cur_size -= stage.size;
                self.mark_published(id);
                return Some(stage.merge());
            } else {
                return None;
            }
        // We got a new chunk that needs to be processed.
        } else {
            self.cur_size += chunk.2.len();
            self.stages.insert(id, MsgStage::new(chunk));
            return None;
        }
    }


}

impl MsgStage {
    fn new(starter: MsgChunk) -> MsgStage {
        let PieceNum(_, out_of) = starter.1;

        let mut stage = MsgStage {
            this_id: starter.0,
            total_pieces: out_of,
            pieces: HashMap::with_capacity(out_of as usize),
            size: 0
        };

        stage.add_chunk(starter);
        stage
    }

    fn is_ready(&self) -> bool {
        self.total_pieces as usize == self.pieces.len()
    }

    fn add_chunk(&mut self, chunk: MsgChunk) -> usize {
        let PieceNum(this, _) = chunk.1;
        if !self.pieces.contains_key(&(this as usize)) {
            let size = chunk.2.len();
            self.size += size;
            self.pieces.insert(this as usize, chunk);
            size
        } else { 0 }
    }

    fn merge(mut self) -> CompleteMessage {
        let mut size = 0;

        for (_, &MsgChunk(_, _, ref bytes)) in self.pieces.iter() {
            size += bytes.len();
        }

        let mut v = Vec::with_capacity(size);

        for (_, &mut MsgChunk(_, _, ref mut bytes)) in self.pieces.iter_mut() {
            for &byte in bytes.iter() {
                v.push(byte);
            }
        }

        CompleteMessage(self.this_id, v)
    }
}


// Stage tests

#[test] fn is_ready_single_complete() {
    let comp_chunk = MsgChunk(MsgId(0), PieceNum(1, 1), vec![0]);
    let stage = MsgStage::new(comp_chunk);
    assert!(stage.is_ready());
    assert!(stage.merge() == CompleteMessage(MsgId(0), vec![0]));
}

#[test] fn is_ready_single_incomplete() {
    let incomp_chunk = MsgChunk(MsgId(0), PieceNum(1, 2), vec![0]);
    let stage = MsgStage::new(incomp_chunk);
    assert!(!stage.is_ready());
}

#[test] fn is_ready_double_complete() {
    let c1 = MsgChunk(MsgId(0), PieceNum(1, 2), vec![0]);
    let c2 = MsgChunk(MsgId(0), PieceNum(2, 2), vec![1]);

    let mut stage = MsgStage::new(c1.clone());
    stage.add_chunk(c2.clone());
    assert!(stage.is_ready());
    assert!(stage.merge() == CompleteMessage(MsgId(0), vec![0, 1]));

    // Now in the opposite order

    let mut stage = MsgStage::new(c2.clone());
    stage.add_chunk(c1.clone());
    assert!(stage.is_ready());
    assert!(stage.merge() == CompleteMessage(MsgId(0), vec![0, 1]));
}

#[test] fn is_ready_double_same() {
    let c1 = MsgChunk(MsgId(0), PieceNum(1, 2), vec![0]);

    let mut stage = MsgStage::new(c1.clone());
    stage.add_chunk(c1);
    assert!(!stage.is_ready());
}

// Queue tests

#[test] fn queue_single() {
    let mut queue = MsgQueue::new(None);
    let c1 = MsgChunk(MsgId(1), PieceNum(1, 1), vec![0]);

    let res = queue.insert_chunk(c1.clone());

    assert!(res.is_some());
    assert!(res.unwrap() == CompleteMessage(MsgId(1), vec![0]));
    assert!(queue.last_released == Some(MsgId(1)));

    // try to requeue the message.  It shouldn't go through this time.
    let res = queue.insert_chunk(c1);
    assert!(res.is_none());
}

#[test] fn queue_double() {
    let mut queue = MsgQueue::new(None);
    let c1 = MsgChunk(MsgId(1), PieceNum(1, 2), vec![0]);
    let c2 = MsgChunk(MsgId(1), PieceNum(2, 2), vec![1]);

    let res = queue.insert_chunk(c1.clone());
    assert!(res.is_none());
    let res = queue.insert_chunk(c2.clone());
    assert!(res.is_some());
    assert!(res.unwrap() == CompleteMessage(MsgId(1), vec![0, 1]));
    assert!(queue.last_released == Some(MsgId(1)));

    assert!(queue.insert_chunk(c1).is_none());
    assert!(queue.insert_chunk(c2).is_none());
}

#[test] fn out_of_order() {
    let mut queue = MsgQueue::new(None);
    let c1 = MsgChunk(MsgId(1), PieceNum(1, 1), vec![0]);
    let c2 = MsgChunk(MsgId(2), PieceNum(1, 1), vec![1]);

    assert!(queue.insert_chunk(c2.clone()).is_some());
    assert!(queue.insert_chunk(c1).is_none());
    assert!(queue.insert_chunk(c2).is_none());
}

#[test] fn odd_orders() {
    let a1 = MsgChunk(MsgId(1), PieceNum(1, 2), vec![0]);
    let a2 = MsgChunk(MsgId(1), PieceNum(2, 2), vec![1]);

    let b1 = MsgChunk(MsgId(2), PieceNum(1, 2), vec![2]);
    let b2 = MsgChunk(MsgId(2), PieceNum(2, 2), vec![3]);

    let mut queue = MsgQueue::new(None);
    assert!(queue.insert_chunk(a1.clone()).is_none());
    assert!(queue.insert_chunk(b1.clone()).is_none());
    assert!(queue.insert_chunk(a2.clone()).is_some());
    assert!(queue.insert_chunk(b2.clone()).is_some());


    let mut queue = MsgQueue::new(None);
    assert!(queue.insert_chunk(a1.clone()).is_none());
    assert!(queue.insert_chunk(b1.clone()).is_none());
    assert!(queue.insert_chunk(b2.clone()).is_some());
    assert!(queue.insert_chunk(a2.clone()).is_none());


    let mut queue = MsgQueue::new(None);
    assert!(queue.insert_chunk(b1.clone()).is_none());
    assert!(queue.insert_chunk(b2.clone()).is_some());
    assert!(queue.insert_chunk(a2.clone()).is_none());
}
