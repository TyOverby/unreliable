use std::sync::mpsc;
use std::collections::{VecDeque, HashMap};
use std::collections::hash_map::Entry;
use std::net::{UdpSocket, ToSocketAddrs, SocketAddr};
use std::rc::Rc;
use std::io::Result as IoResult;

use super::msgqueue::*;
use super::{UnrError, UnrResult};
use bincode;

static MSG_PADDING: u16 = 32;

/// The sending end of an unreliable message socket.
pub struct Sender {
    out_queue: VecDeque<(MsgChunk, Rc<AddrsContainer>)>,
    last_id: u64,
    socket: UdpSocket,
    pub datagram_length: u16,
    pub replication: u8
}

/// The receiving end of an unreliable message socket.
pub struct Receiver {
    socket: UdpSocket,
    queue: HashMap<SocketAddr, MsgQueue>,
    pub datagram_length: u16
}

#[derive(Debug)]
struct AddrsContainer{
    v: Vec<SocketAddr>
}

impl AddrsContainer {
    fn from_to_sock<T: ToSocketAddrs>(socket_addrs: T) -> IoResult<AddrsContainer> {
        let iter = try!(socket_addrs.to_socket_addrs());
        let vec = iter.collect();
        Ok(AddrsContainer{v: vec})
    }
}

impl ToSocketAddrs for AddrsContainer {
    type Iter = ::std::vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> IoResult<<AddrsContainer as ToSocketAddrs>::Iter> {
        let slice: Vec<_> = self.v[..].iter().cloned().collect();
        Ok(slice.into_iter())
    }
}

impl Receiver {
    /// Constructs a receiver from a socket.
    ///
    /// `datagram_length` is the max-size of the UDP packet that you expect to
    /// receive.
    pub fn from_socket(socket: UdpSocket, datagram_length: u16) -> Receiver {
        Receiver {
            socket: socket,
            datagram_length: datagram_length,
            queue: HashMap::new()
        }
    }

    /// Blocks until a completed message is received, and returns the Socket
    /// Address that the message came from.
    pub fn poll(&mut self) -> UnrResult<(SocketAddr, CompleteMessage)> {
        let mut buf: Vec<u8> = (0 .. self.datagram_length).map(|_| 0).collect();
        loop {
            let (amnt, from) = try!(self.socket.recv_from(&mut buf[..]));
            let data = &buf[0 .. amnt];
            let chunk: MsgChunk = try!(bincode::decode(data));

            let q = self.queue.entry(from.clone()).or_insert_with(|| MsgQueue::new());
            if let Some(completed) = q.insert_chunk(chunk) {
                return Ok((from, completed));
            }
        }
    }
}

impl Sender {
    /// Constructs a sender from a socket.
    ///
    /// * `datagram_length` is the max-size of a UDP packet.
    /// * `replication` is the amout of times that a chunk will get re-sent.
    ///
    /// `replication` should almost always be `1`, and rarely `2` or above.
    pub fn from_socket(socket: UdpSocket, datagram_length: u16, replication: u8) -> Sender {
        Sender {
            out_queue: VecDeque::new(),
            last_id: 0,
            socket: socket,
            datagram_length: datagram_length,
            replication: replication
        }
    }

    /// Adds a message to the queue of chunks to send out.
    pub fn enqueue<T: ToSocketAddrs>(&mut self, message: Vec<u8>, addrs: T) -> UnrResult<()> {
        self.last_id += 1;
        let id = self.last_id;
        let addrs = Rc::new(try!(AddrsContainer::from_to_sock(addrs)));
        let num_chunks = message.len() / ((self.datagram_length - MSG_PADDING) as usize);

        for _ in 0 .. self.replication {
            let mut chunk_count = 0;
            for chunk in message[..].chunks((self.datagram_length - MSG_PADDING) as usize) {
                let mut v = Vec::new();
                v.push_all(chunk);
                let chunk = MsgChunk(
                    MsgId(id), PieceNum(chunk_count + 1, (num_chunks + 1) as u16), v);
                self.out_queue.push_back((chunk, addrs.clone()));
                chunk_count += 1;
            }
        }

        Ok(())
    }

    /// Attempts to send one UDP packet over the network.
    ///
    /// The size of the UDP packet is bounded by `self.datagram_length`.
    ///
    /// ## Returns
    /// * Err(e) if an error occurred during sending.
    /// * Ok(true) if there are more messages in the queue.
    /// * Ok(false) if theere are no more messages in the queue.
    pub fn send_one(&mut self) -> UnrResult<bool> {
        let bound = bincode::SizeLimit::Bounded(self.datagram_length as u64);
        if let Some((next, addrs)) = self.out_queue.pop_front() {
            let bytes = try!(bincode::encode(&next, bound));
            try!(self.socket.send_to(&bytes[..], &*addrs));
        }

        Ok(!self.out_queue.is_empty())
    }

    /// Attemts to send all UDP packets by repeatedly calling `send_one`.
    pub fn send_all(&mut self) -> UnrResult<()> {
        while try!(self.send_one()) {}
        Ok(())
    }
}