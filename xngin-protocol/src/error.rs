use crate::mysql::packet::ErrPacket;
use std::str::Utf8Error;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("Serialize/Deserialize error: {0}")]
    SerdeError(String),
    #[error("IO error: {0}")]
    IOError(std::io::ErrorKind),
    #[error("Insufficient input")]
    InsufficientInput,
    #[error("Invalid input")]
    InvalidInput,
    #[error("Unexpected EOF")]
    UnexpectedEOF,
    #[error("Exceeds capacity with additional {0} bytes")]
    ExceedsCapacity(usize),
    #[error("Invalid command code {0}")]
    InvalidCommandCode(u8),
    #[error("Invalid status flags {0}")]
    InvalidStatusFlags(u16),
    #[error("Invalid packet code {0}")]
    InvalidPacketCode(u8),
    #[error("Invalid packet code {0} != {1}")]
    InvalidPacketLength(usize, usize),
    #[error("Invalid column type {0}")]
    InvalidColumnType(u8),
    #[error("Invalid UTF-8 string")]
    InvalidUtf8String,
    #[error("Invalid RSA key string")]
    InvalidRSAKeyString,
    #[error("Packet Number mismatch {0} != {1}")]
    PacketNumberMismatch(u8, u8),
    #[error("Invalid buffer state")]
    InvalidBufferState,
    #[error("Invalid execute query response")]
    InvalidExecResp,
    #[error("Malformed packet")]
    MalformedPacket,
    #[error("Empty packet payload")]
    EmptyPacketPayload,
    #[error("Buffer full, expected {expected:}")]
    BufferFull { expected: usize },
    #[error("Packet split")]
    PacketSplit,
    #[error("SQL error, code={code:}, marker={marker:} state_and_msg={state_and_msg:?}")]
    SqlError {
        code: u16,
        marker: u8,
        state_and_msg: Box<(String, String)>,
    },
    #[error("Empty fast auth result")]
    EmptyFastAuthResult,
    #[error("Unknown server response after fast auth")]
    UnknownServerRespAfterFastAuth,
    #[error("{0} unimplemented")]
    Unimplemented(&'static str),
    #[error("Auth plugin '{0}' not supported")]
    AuthPluginNotSupported(String),
    #[error("Unsupported protocol")]
    UnsupportedProtocol,
    #[error("RSA error")]
    RSAError,
    #[error("SPKI error")]
    SPKIError,
}

impl From<crate::buf::Error> for Error {
    #[inline]
    fn from(src: crate::buf::Error) -> Self {
        match src {
            crate::buf::Error::ExceedsCapacity(n) => Error::ExceedsCapacity(n),
            crate::buf::Error::InvalidInput => Error::InvalidInput,
            crate::buf::Error::InvalidState => Error::InvalidBufferState,
        }
    }
}

impl From<std::io::Error> for Error {
    #[inline]
    fn from(src: std::io::Error) -> Self {
        Error::IOError(src.kind())
    }
}

impl<'a> From<ErrPacket<'a>> for Error {
    #[inline]
    fn from(src: ErrPacket) -> Self {
        let state = String::from_utf8_lossy(&src.sql_state).into_owned();
        let msg = String::from_utf8_lossy(&src.error_message).into_owned();
        Error::SqlError {
            code: src.error_code,
            marker: src.sql_state_marker,
            state_and_msg: Box::new((state, msg)),
        }
    }
}

impl From<Utf8Error> for Error {
    #[inline]
    fn from(_src: Utf8Error) -> Self {
        Error::InvalidUtf8String
    }
}

impl From<spki::Error> for Error {
    #[inline]
    fn from(_src: spki::Error) -> Self {
        Error::SPKIError
    }
}

impl From<rsa::errors::Error> for Error {
    #[inline]
    fn from(_src: rsa::errors::Error) -> Self {
        Error::RSAError
    }
}