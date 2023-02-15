use crate::error::{Error, Result};
use crate::mysql::flag::*;
use crate::mysql::serde::{
    LenEncInt, MyDeser, MyDeserExt, MySerElem, MySerPacket, NewMySer, SerdeCtx,
};
use std::borrow::Cow;

/// Ok Packet
///
/// reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OkPacket<'a> {
    pub header: u8,
    // actually len-enc-int
    pub affected_rows: u64,
    // actually len-enc-int
    pub last_insert_id: u64,
    // if PROTOCOL_41 or TRANSACTIONS enabled
    pub status_flags: StatusFlags,
    // if PROTOCOL_41 enabled
    pub warnings: u16,
    // if SESSION_TRACK enabled: len-enc-str
    // else: EOF-terminated string
    pub info: Cow<'a, [u8]>,
    // if SESSION_TRACK and SESSION_STATE_CHANGED enabled
    pub session_state_changes: Cow<'a, [u8]>,
}

impl<'a> NewMySer for OkPacket<'a> {
    type Ser<'s> = MySerPacket<'s, 7> where Self: 's;

    #[inline]
    fn new_my_ser(&self, ctx: &mut SerdeCtx) -> Self::Ser<'_> {
        // u8 header
        let header = MySerElem::one_byte(self.header);
        // len-enc-int affected rows
        let affected_rows = MySerElem::len_enc_int(LenEncInt::from(self.affected_rows));
        // len-enc-int last insert id
        let last_insert_id = MySerElem::len_enc_int(LenEncInt::from(self.last_insert_id));
        // if PROTOCOL_41 or TRANSACTIONS enabled
        let status_flags = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41)
            || ctx.cap_flags.contains(CapabilityFlags::TRANSACTIONS)
        {
            MySerElem::le_u16(self.status_flags.bits())
        } else {
            MySerElem::empty()
        };
        // if PROTOCOL_41 enabled
        let warnings = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            MySerElem::le_u16(self.warnings)
        } else {
            MySerElem::empty()
        };
        // if SESSION_TRACK enabled: len-enc-str
        // else: EOF-terminated string
        let info = if ctx.cap_flags.contains(CapabilityFlags::SESSION_TRACK) {
            MySerElem::len_enc_str(&*self.info)
        } else {
            MySerElem::slice(&self.info)
        };
        // if SESSION_TRACK and SESSION_STATE_CHANGED enabled
        let session_state_changes = if ctx.cap_flags.contains(CapabilityFlags::SESSION_TRACK)
            && self
                .status_flags
                .contains(StatusFlags::SESSION_STATE_CHANGED)
        {
            MySerElem::len_enc_str(&*self.session_state_changes)
        } else {
            MySerElem::empty()
        };
        MySerPacket::new(
            ctx,
            [
                header,
                affected_rows,
                last_insert_id,
                status_flags,
                warnings,
                info,
                session_state_changes,
            ],
        )
    }
}

impl<'a> MyDeser<'a> for OkPacket<'a> {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        // header can be either 0x00 or 0xfe
        let header = input.try_deser_u8()?;
        if header != 0x00 && header != 0xfe {
            return Err(Error::MalformedPacket);
        }
        let affected_rows = input.try_deser_len_enc_int()?.try_into()?;
        let last_insert_id = input.try_deser_len_enc_int()?.try_into()?;
        let status_flags = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41)
            || ctx.cap_flags.contains(CapabilityFlags::TRANSACTIONS)
        {
            let status_flags = input.try_deser_le_u16()?;
            StatusFlags::from_bits(status_flags).ok_or(Error::InvalidStatusFlags(status_flags))?
        } else {
            StatusFlags::empty()
        };
        let warnings = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            input.try_deser_le_u16()?
        } else {
            0
        };
        let info = if ctx.cap_flags.contains(CapabilityFlags::SESSION_TRACK) {
            input.try_deser_len_enc_str()?.try_into()?
        } else {
            input.deser_to_end()
        };
        let session_state_changes = if ctx.cap_flags.contains(CapabilityFlags::SESSION_TRACK)
            && status_flags.contains(StatusFlags::SESSION_STATE_CHANGED)
        {
            input.try_deser_len_enc_str()?.try_into()?
        } else {
            &[][..]
        };
        let res = OkPacket {
            header,
            affected_rows,
            last_insert_id,
            status_flags,
            warnings,
            info: Cow::Borrowed(info),
            session_state_changes: Cow::Borrowed(session_state_changes),
        };
        Ok((*input, res))
    }
}

impl_from_ref!(OkPacket: header, affected_rows, last_insert_id, status_flags, warnings; info, session_state_changes);

/// Err Packet
///
/// reference: https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrPacket<'a> {
    // pub header: u8,
    pub error_code: u16,
    // if PROTOCOL_41 enabled: string[1]
    // pub sql_state_marker: u8,
    // if PROTOCOL_41 enabled: string[5]
    pub sql_state: Cow<'a, [u8]>,
    // EOF-terminated string
    pub error_message: Cow<'a, [u8]>,
}

impl<'a> NewMySer for ErrPacket<'a> {
    type Ser<'s> = MySerPacket<'s, 3> where Self: 's;

    #[inline]
    fn new_my_ser(&self, ctx: &mut SerdeCtx) -> Self::Ser<'_> {
        let mut elem0 = [0u8; 4]; // combine header, error code, state marker
        elem0[0] = 0xff;
        elem0[1] = self.error_code as u8;
        elem0[2] = (self.error_code >> 8) as u8;
        let sql_state = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            // set marker to '#'
            elem0[3] = b'#';
            MySerElem::slice(&self.sql_state)
        } else {
            MySerElem::empty()
        };
        MySerPacket::new(
            ctx,
            [
                MySerElem::inline_bytes(&elem0),
                sql_state,
                MySerElem::slice(&self.error_message),
            ],
        )
    }
}

impl<'a> MyDeser<'a> for ErrPacket<'a> {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let header = input.try_deser_u8()?;
        if header != 0xff {
            return Err(Error::MalformedPacket);
        }
        let error_code = input.try_deser_le_u16()?;
        // handshake stage has different behavior?
        let sql_state = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            let marker = input.try_deser_u8()?;
            if marker != b'#' {
                return Err(Error::MalformedPacket);
            }
            input.try_deser_bytes(5usize)?
        } else {
            &[]
        };
        let error_message = input.deser_to_end();
        let res = ErrPacket {
            error_code,
            sql_state: Cow::Borrowed(sql_state),
            error_message: Cow::Borrowed(error_message),
        };
        Ok((*input, res))
    }
}

impl_from_ref!(ErrPacket: error_code; sql_state, error_message);

impl<'a> From<&'a Error> for ErrPacket<'a> {
    #[inline]
    fn from(src: &'a Error) -> Self {
        match src {
            Error::SqlError {
                code,
                state_and_msg,
            } => ErrPacket {
                error_code: *code,
                sql_state: Cow::Borrowed(state_and_msg.0.as_bytes()),
                error_message: Cow::Borrowed(state_and_msg.1.as_bytes()),
            },
            _ => ErrPacket {
                error_code: 0,
                sql_state: Cow::Borrowed(b""),
                error_message: Cow::Owned(src.to_string().into_bytes()),
            },
        }
    }
}

/// EOF Packet
/// This kind of packet is deprecated.
/// Since MySQL 5.7.5, it's replace with Ok Packet.
///
/// reference: https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EofPacket {
    pub header: u8,
    // if PROTOCOL_41 enabled
    pub warnings: u16,
    // if PROTOCOL_41 enabled
    pub status_flags: StatusFlags,
}

impl NewMySer for EofPacket {
    type Ser<'s> = MySerPacket<'s, 3> where Self: 's;
    #[inline]
    fn new_my_ser(&self, ctx: &mut SerdeCtx) -> Self::Ser<'_> {
        let (warnings, status_flags) = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            (
                MySerElem::le_u16(self.warnings),
                MySerElem::le_u16(self.status_flags.bits()),
            )
        } else {
            (MySerElem::empty(), MySerElem::empty())
        };
        MySerPacket::new(
            ctx,
            [MySerElem::one_byte(self.header), warnings, status_flags],
        )
    }
}

impl<'a> MyDeser<'a> for EofPacket {
    #[inline]
    fn my_deser(ctx: &mut SerdeCtx, mut input: &'a [u8]) -> Result<(&'a [u8], Self)> {
        let input = &mut input;
        let header = input.try_deser_u8()?;
        let (warnings, status_flags) = if ctx.cap_flags.contains(CapabilityFlags::PROTOCOL_41) {
            let warnings = input.try_deser_le_u16()?;
            let status_flags = input.try_deser_le_u16()?;
            let status_flags = StatusFlags::from_bits(status_flags)
                .ok_or(Error::InvalidStatusFlags(status_flags))?;
            (warnings, status_flags)
        } else {
            (0, StatusFlags::empty())
        };
        let res = EofPacket {
            header,
            warnings,
            status_flags,
        };
        Ok((*input, res))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buf::ByteBuffer;
    use crate::mysql::serde::tests::check_ser_and_deser;

    #[test]
    fn test_serde_ok_packet() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let ok = OkPacket {
            header: 0,
            affected_rows: 1,
            last_insert_id: 3,
            status_flags: StatusFlags::STATUS_AUTOCOMMIT,
            warnings: 0,
            info: Cow::Borrowed(b""),
            session_state_changes: Cow::Borrowed(b""),
        };
        check_ser_and_deser(&mut ctx, &ok, &buf);
    }

    #[test]
    fn test_serde_err_packet() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let err = ErrPacket {
            error_code: 1304,
            sql_state: Cow::Borrowed(b"01S01"),
            error_message: Cow::Borrowed(b"internal error"),
        };
        check_ser_and_deser(&mut ctx, &err, &buf);
    }

    #[test]
    fn test_serde_eof_packet() {
        let mut ctx = SerdeCtx::default();
        let buf = ByteBuffer::with_capacity(1024);
        let eof = EofPacket {
            header: 0,
            status_flags: StatusFlags::STATUS_AUTOCOMMIT,
            warnings: 0,
        };
        check_ser_and_deser(&mut ctx, &eof, &buf);
    }
}
