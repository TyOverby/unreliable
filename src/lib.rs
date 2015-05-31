#![allow(unused)]

extern crate bincode;
extern crate rustc_serialize;

use std::io::Error as IoError;
use bincode::{EncodingError, DecodingError};
pub use network::{Sender, Receiver};

pub mod msgqueue;
pub mod network;

pub type UnrResult<T> = Result<T, UnrError>;

#[derive(Debug)]
pub enum UnrError {
    IoError(IoError),
    EncodingError(EncodingError),
    DecodingError(DecodingError)
}

impl From<IoError> for UnrError {
    fn from(ioe: IoError) -> UnrError {
        UnrError::IoError(ioe)
    }
}

impl From<EncodingError> for UnrError {
    fn from(e: EncodingError) -> UnrError {
        UnrError::EncodingError(e)
    }
}

impl From<DecodingError> for UnrError {
    fn from(e: DecodingError) -> UnrError {
        UnrError::DecodingError(e)
    }
}
