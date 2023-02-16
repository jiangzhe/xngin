mod de;
mod ser;

use crate::buf::ByteBuffer;
use crate::error::{Error, Result};
use crate::mysql::cmd::CmdCode;
use crate::mysql::flag::{CapabilityFlags, StatusFlags};
use semistr::SemiStr;

pub use de::{my_deser_packet, MyDeser, MyDeserExt};
pub use ser::{MySer, MySerElem, MySerExt, MySerKind, MySerPacket, NewMySer};

/// Server and client modes have different behaviors.
pub enum SerdeMode {
    Server,
    Client,
}

/// Context contains session level runtime status, user variables
/// and buffer to store intermediate data.
pub struct SerdeCtx {
    /// Capability flags indicates the expectation and behavior
    /// of both server and client.
    pub cap_flags: CapabilityFlags,
    /// Status flags indicates server status, e.g. transaction
    /// status, cursor status, result set status, etc.
    pub status_flags: StatusFlags,
    /// local buffers, storing temporary serialization data.
    /// They must be released once the serialization is done.
    bufs: Vec<(*mut u8, usize)>,
    /// current command code if exists.
    /// this may be used to distinguish deserialization behavior.
    pub curr_cmd: Option<CmdCode>,
    /// Packet number starts from 0, auto increment by 1 per packet.
    /// Wrapped if overflow.
    pkt_nr: u8,
    /// Maximum payload size, used to split packets if payload is too long.
    /// It should not be changed once the context is used.
    /// Otherwise, the serializaion/deserialization may be wrong.
    pub(crate) max_payload_size: usize,

    pub mode: SerdeMode,
}

unsafe impl Send for SerdeCtx {}
unsafe impl Sync for SerdeCtx {}

impl Default for SerdeCtx {
    #[inline]
    fn default() -> Self {
        SerdeCtx {
            cap_flags: CapabilityFlags::default(),
            status_flags: StatusFlags::empty(),
            bufs: vec![],
            curr_cmd: None,
            pkt_nr: 0,
            max_payload_size: 0xffffff,
            mode: SerdeMode::Client, // by default use client mode
        }
    }
}

impl SerdeCtx {
    #[inline]
    pub fn with_mode(mut self, mode: SerdeMode) -> Self {
        self.mode = mode;
        self
    }

    /// Update max payload size.
    /// This method is used only for test.
    #[cfg(test)]
    #[inline]
    pub fn set_max_payload_size(&mut self, size: usize) {
        self.max_payload_size = size;
    }

    /// Reset packet number to zero.
    /// There are two cases to reset packet number:
    /// 1. After handshake process is completed.
    /// 2. Before each command sent from client.
    #[inline]
    pub fn reset_pkt_nr(&mut self) {
        self.pkt_nr = 0;
    }

    #[inline]
    pub fn check_and_inc_pkt_nr(&mut self, pkt_nr: u8) -> Result<()> {
        if self.pkt_nr != pkt_nr {
            return Err(Error::PacketNumberMismatch(pkt_nr, self.pkt_nr));
        }
        self.pkt_nr = self.pkt_nr.wrapping_add(1);
        Ok(())
    }

    /// This method will move the ownership of buffer to context,
    /// and returns its immutable reference.
    /// The context will put it in a buffer list and deallocate
    /// when calling [`SerdeCtx::release_bufs`] method.
    ///
    /// # Safety
    ///
    /// It's unsafe because the lifetime of returned slice is
    /// arbitrary. It's user's duty to keep the buffer outlive
    /// the reference.
    /// The returned slice is only the readable part of this buffer.
    #[inline]
    pub unsafe fn accept_buf<'a>(&mut self, buf: ByteBuffer) -> &'a [u8] {
        let (ptr, len, r_idx, w_idx) = buf.into_raw_parts();
        let s = std::slice::from_raw_parts(ptr.add(r_idx), w_idx - r_idx);
        self.bufs.push((ptr, len));
        s
    }

    /// Release buffers.
    ///
    /// # Safety
    ///
    /// User should make sure all references to buffers are dropped before
    /// calling this method.
    #[inline]
    pub unsafe fn release_bufs(&mut self) {
        for (ptr, len) in self.bufs.drain(..).rev() {
            drop(ByteBuffer::from_raw_parts(ptr, len))
        }
    }
}

impl Drop for SerdeCtx {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.release_bufs() }
    }
}

/// MySQL length encoded integer.
/// Meaning of 1-byte prefix:
/// 0x00 ~ 0xfa => 1-byte integer.
/// 0xfb        => Null.
/// 0xfc        => followed by 2-byte integer.
/// 0xfd        => followed by 3-byte integer.
/// 0xfe        => followed by 8-byte integer.
/// 0xff        => Error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LenEncInt {
    Null,
    Err,
    Len1(u8),
    Len3(u16),
    Len4(u32),
    Len9(u64),
}

impl LenEncInt {
    #[allow(clippy::len_without_is_empty)]
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            LenEncInt::Null | LenEncInt::Err | LenEncInt::Len1(_) => 1,
            LenEncInt::Len3(_) => 3,
            LenEncInt::Len4(_) => 4,
            LenEncInt::Len9(_) => 9,
        }
    }
}

impl TryFrom<LenEncInt> for u64 {
    type Error = Error;
    #[inline]
    fn try_from(src: LenEncInt) -> Result<Self> {
        match src {
            LenEncInt::Len1(n) => Ok(n as u64),
            LenEncInt::Len3(n) => Ok(n as u64),
            LenEncInt::Len4(n) => Ok(n as u64),
            LenEncInt::Len9(n) => Ok(n as u64),
            _ => Err(Error::InvalidInput),
        }
    }
}

/// convert u64 to len-enc-int
impl From<u64> for LenEncInt {
    fn from(src: u64) -> Self {
        if src < 0xfb {
            LenEncInt::Len1(src as u8)
        } else if src <= 0xffff {
            LenEncInt::Len3(src as u16)
        } else if src <= 0xffffff {
            LenEncInt::Len4(src as u32)
        } else {
            LenEncInt::Len9(src)
        }
    }
}

/// convert u8 to len-enc-int
impl From<u8> for LenEncInt {
    fn from(src: u8) -> Self {
        LenEncInt::Len1(src)
    }
}

/// convert u16 to len-enc-int
impl From<u16> for LenEncInt {
    fn from(src: u16) -> Self {
        if src <= 0xfb {
            LenEncInt::Len1(src as u8)
        } else {
            LenEncInt::Len3(src)
        }
    }
}

/// convert u32 to len-enc-int
impl From<u32> for LenEncInt {
    fn from(src: u32) -> Self {
        if src <= 0xfb {
            LenEncInt::Len1(src as u8)
        } else if src <= 0xffff {
            LenEncInt::Len3(src as u16)
        } else {
            LenEncInt::Len4(src as u32)
        }
    }
}

/// MySQL length encoded string.
/// Prefixed a length encoded integer, indicating the string length.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LenEncStr<'a> {
    Null,
    Err,
    Bytes(&'a [u8]),
}

impl<'a> LenEncStr<'a> {
    #[inline]
    pub fn into_bytes(self) -> Option<&'a [u8]> {
        match self {
            LenEncStr::Bytes(bs) => Some(bs),
            _ => None,
        }
    }
}

impl<'a> TryFrom<LenEncStr<'a>> for &'a [u8] {
    type Error = Error;
    #[inline]
    fn try_from(src: LenEncStr<'a>) -> Result<Self> {
        match src {
            LenEncStr::Bytes(bs) => Ok(bs),
            _ => Err(Error::InvalidInput),
        }
    }
}

impl TryFrom<LenEncStr<'_>> for SemiStr {
    type Error = Error;
    #[inline]
    fn try_from(src: LenEncStr) -> Result<Self> {
        match src {
            LenEncStr::Bytes(bs) => {
                let s = std::str::from_utf8(bs)?;
                SemiStr::try_from(s).map_err(|_| Error::StringTooLong)
            }
            _ => Err(Error::InvalidInput),
        }
    }
}

impl<'a> From<&'a [u8]> for LenEncStr<'a> {
    #[inline]
    fn from(src: &'a [u8]) -> Self {
        LenEncStr::Bytes(src)
    }
}

impl<'a> From<&'a str> for LenEncStr<'a> {
    #[inline]
    fn from(src: &'a str) -> Self {
        LenEncStr::Bytes(src.as_bytes())
    }
}

pub trait LenEncStrExt {
    /// Returns number of bytes if converted to len-enc-str
    fn len_enc_str_len(&self) -> usize;
}

impl LenEncStrExt for [u8] {
    #[inline]
    fn len_enc_str_len(&self) -> usize {
        let slen = self.len();
        LenEncInt::from(slen as u64).len() + slen
    }
}

pub(crate) mod tests {
    use super::*;

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn check_ser_and_deser<
        'a,
        T: NewMySer + MyDeser<'a> + PartialEq + std::fmt::Debug,
    >(
        ctx: &mut SerdeCtx,
        val: &T,
        buf: &'a ByteBuffer,
    ) {
        // serialize
        check_ser(ctx, val, buf);
        // deserialize
        let v2: T = check_deser(ctx, buf);
        assert_eq!(val, &v2);
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn check_ser<T: NewMySer>(ctx: &mut SerdeCtx, val: &T, buf: &ByteBuffer) {
        // serialize
        ctx.reset_pkt_nr();
        let (writable, _wg) = buf.writable().unwrap();
        let orig_len = writable.len();
        let writable = val.new_my_ser(ctx).my_ser(ctx, writable);
        buf.advance_w_idx(orig_len - writable.len()).unwrap();
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn check_deser<'a, T: MyDeser<'a>>(ctx: &mut SerdeCtx, buf: &'a ByteBuffer) -> T {
        // deserialize
        ctx.reset_pkt_nr();
        let (readable, rg) = buf.readable().unwrap();
        let res = my_deser_packet(ctx, readable).unwrap();
        rg.advance(readable.len());
        res
    }
}
