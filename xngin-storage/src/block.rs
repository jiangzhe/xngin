use crate::codec::{Codec, SingleCodec};
use crate::error::{Error, Result};
use smallvec::{smallvec, SmallVec};
use std::io;
use std::num::NonZeroU64;
use std::sync::Arc;
use xngin_common::psma::PSMA;
use xngin_datatype::PreciseType;

/// Block collects multiple tuples and aggregate synopses for analytical query.
/// On-disk format is described as below.
///
/// Block:
/// 1. Number of records: 2B with 6B padding.
/// 2. Number of attributes: 2B with 6B padding.
/// 3. Attribute metadata, ..., Attribute metadata: N * 48 B.
/// 4. Attribute data, ..., Attribute data: variable length.
///
/// Attribute metadata:
/// 1. Attribute precise type: 8B.
/// 2. Compression method: 2B with 6B padding.
/// 3. Offset SMA: 8B.
/// 4. Offset dict: 8B. 0 means there is no dict.
/// 5. Offset data: 8B.
/// 6. Offset string: 8B. 0 means there is no string.
///
/// All offsets listed above start from first byte this block.
/// Attribute metadata has length 40B in total.
///
/// Attribute data:
/// 1. SMA data: contains MinValue and MaxValue, length depends on data type.
/// 2. Lookup(PSMA) table: lookup by first non-zero byte, length depends on data type.
/// 3. Dict data
/// 4. Compressed data: fixed-length data, length depends on compression method.
/// 5. String data: variable-length data.
///
/// Lookup table:
/// 1. Entry count: 2B with 6B padding.
/// 2. Lookup ranges: 256 * 4 * 2 * N B. N is byte width of value. e.g. N of u16 is 2.
///                   Key of lookup range is first non-zero byte. Range contains
///                   start offset and end offset(exclusive). end offset = 0 means missing.
///
/// Dict data:
/// 1. Entry count: 4B with 4B padding.
/// 2. Entry offsets: (N + 1) * 4 B, with padding to make multiple of 8.
///                   Offset starts from first byte of "String data".
///
/// Single Codec:
/// 1. Number of bytes: 4B.
/// 2. Number of identical values: 4B.
/// 3. Value Bytes: variable number of bytes.
pub struct Block {
    pub len: usize,
    pub attrs: Vec<Attr>,
}

impl Block {
    #[inline]
    pub fn fetch_attr(&self, ext_idx: usize) -> Option<Attr> {
        self.attrs.get(ext_idx).map(Attr::to_owned)
    }

    #[inline]
    fn persist<W: io::Write>(&self, writer: &mut W, buf: &mut Vec<u8>) -> io::Result<usize> {
        buf.clear();
        // number of records
        buf.extend_from_slice(&u16::to_ne_bytes(self.len as u16));
        buf.extend_from_slice(&[0u8; 6]); // padding
                                          // number of attributes
        buf.extend_from_slice(&u16::to_ne_bytes(self.attrs.len() as u16));
        buf.extend_from_slice(&[0u8; 6]); // padding
                                          // preserve metadata offset
        let mut offset = 16u64 + (self.attrs.len() * LEN_ATTR_META) as u64;
        let mut expected_lens = Vec::with_capacity(self.attrs.len());
        for attr in &self.attrs {
            let (meta, new_offset) = attr.calc_meta(offset);
            // precise type
            buf.extend_from_slice(&<[u8; 8]>::from(meta.ty));
            // compression method
            buf.extend_from_slice(&<[u8; 2]>::from(meta.cmprs_mthd));
            buf.extend_from_slice(&[0u8; 6]); // padding
                                              // offset sma
            buf.extend_from_slice(
                &meta
                    .offset_sma
                    .map(|v| v.get())
                    .unwrap_or_default()
                    .to_ne_bytes(),
            );
            // offset dict
            buf.extend_from_slice(
                &meta
                    .offset_dict
                    .map(|v| v.get())
                    .unwrap_or_default()
                    .to_ne_bytes(),
            );
            // offset data
            buf.extend_from_slice(&meta.offset_data.get().to_ne_bytes());
            // offset str
            buf.extend_from_slice(
                &meta
                    .offset_str
                    .map(|v| v.get())
                    .unwrap_or_default()
                    .to_ne_bytes(),
            );
            expected_lens.push(new_offset - offset);
            offset = new_offset;
        }
        // persist metadata
        writer.write_all(buf)?;
        buf.clear();
        // persist each attribute
        for (attr, expected_len) in self.attrs.iter().zip(expected_lens) {
            let len = attr.persist(writer, buf)?;
            assert_eq!(expected_len as usize, len);
        }
        writer.flush()?;
        Ok(offset as usize)
    }
}

pub struct Attr {
    pub ty: PreciseType,
    pub codec: Codec,
    pub psma: Option<PSMA>,
}

impl Attr {
    #[inline]
    pub fn to_owned(&self) -> Self {
        Attr {
            ty: self.ty,
            codec: self.codec.to_owned(),
            psma: None,
        }
    }

    #[inline]
    pub fn calc_meta(&self, offset: u64) -> (AttrMeta, u64) {
        match &self.codec {
            Codec::Single(s) => {
                let offset_data = NonZeroU64::new(offset).unwrap();
                let data_len = s.persist_len() as u64;
                let meta = AttrMeta {
                    ty: self.ty,
                    cmprs_mthd: CompressMethod::Single,
                    offset_sma: None,
                    offset_dict: None,
                    offset_data,
                    offset_str: None,
                };
                (meta, align_u128(offset + data_len))
            }
            Codec::Flat(_) => todo!(),
        }
    }

    #[inline]
    pub fn persist<W: io::Write>(&self, writer: &mut W, buf: &mut Vec<u8>) -> io::Result<usize> {
        match &self.codec {
            Codec::Single(s) => {
                buf.clear();
                // number of bytes
                // if null, set to u32::MAX
                let n_bytes = if !s.is_valid() {
                    u32::MAX
                } else {
                    s.data_len() as u32
                };
                buf.extend_from_slice(&n_bytes.to_ne_bytes());
                buf.extend_from_slice(&(s.len() as u32).to_ne_bytes());
                buf.extend_from_slice(s.raw_data());
                let buf_len = buf.len() as u64;
                for _ in buf_len..align_u128(buf_len) {
                    buf.push(0);
                }
                writer.write_all(buf)?;
                Ok(buf.len())
            }
            Codec::Flat(_) => todo!(),
        }
    }
}

#[inline]
fn align_u128(v: u64) -> u64 {
    (v + 15) & !15
}

// block level offset
const OFFSET_START_N_RECORDS: usize = 0;
const OFFSET_END_N_RECORDS: usize = 2;
const OFFSET_START_N_ATTRS: usize = 8;
const OFFSET_END_N_ATTRS: usize = 10;
const OFFSET_START_ATTR_META: usize = 16;
// attribute metadata level offset
const ATTR_META_OFFSET_START_TY: usize = 0;
const ATTR_META_OFFSET_END_TY: usize = 8;
const ATTR_META_OFFSET_START_CMPRS_MTHD: usize = 8;
const ATTR_META_OFFSET_END_CMPRS_MTHD: usize = 10;
const ATTR_META_OFFSET_START_SMA: usize = 16;
const ATTR_META_OFFSET_END_SMA: usize = 24;
const ATTR_META_OFFSET_START_DICT: usize = 24;
const ATTR_META_OFFSET_END_DICT: usize = 32;
const ATTR_META_OFFSET_START_DATA: usize = 32;
const ATTR_META_OFFSET_END_DATA: usize = 40;
const ATTR_META_OFFSET_START_STR: usize = 40;
const ATTR_META_OFFSET_END_STR: usize = 48;
const LEN_ATTR_META: usize = 48;
// single codec offset
const SG_OFFSET_START_BYTES: usize = 0;
const SG_OFFSET_END_BYTES: usize = 4;
const SG_OFFSET_START_VALUES: usize = 4;
const SG_OFFSET_END_VALUES: usize = 8;

/// RawBlock is the serialized byte format of Block, which could
/// be written directly to disk, and obviously also could be
/// read directly from disk.
#[derive(Clone)]
pub struct RawBlock {
    /// Reference counted byte array.
    /// All data derived from this byte array must clone the
    /// reference counter to make sure data is immutable and
    /// always valid through the whole lifetime.
    inner: Arc<[u8]>,
}

impl RawBlock {
    #[inline]
    pub fn new(bytes: Arc<[u8]>) -> Self {
        Self { inner: bytes }
    }

    /// Returns bytes of underlying byte array.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns number of records.
    #[inline]
    pub fn n_records(&self) -> u16 {
        u16::from_ne_bytes(
            self.inner[OFFSET_START_N_RECORDS..OFFSET_END_N_RECORDS]
                .try_into()
                .unwrap(),
        )
    }

    /// Returns number of attributes.
    #[inline]
    pub fn n_attrs(&self) -> u16 {
        u16::from_ne_bytes(
            self.inner[OFFSET_START_N_ATTRS..OFFSET_END_N_ATTRS]
                .try_into()
                .unwrap(),
        )
    }

    /// Returns attribute metadata.
    #[inline]
    fn attr_meta(&self, attr_id: usize) -> Option<AttrMeta> {
        if attr_id >= self.n_attrs() as usize {
            return None;
        }
        let start = OFFSET_START_ATTR_META + attr_id * LEN_ATTR_META;
        let end = start + LEN_ATTR_META;
        let attr_meta = AttrMeta::try_from(&self.inner[start..end]).unwrap();
        Some(attr_meta)
    }

    #[inline]
    pub fn attr(&self, attr_id: usize) -> Result<Attr> {
        self.attr_meta(attr_id)
            .ok_or(Error::InvalidFormat)
            .and_then(|meta| match meta.cmprs_mthd {
                CompressMethod::Single => {
                    debug_assert!(meta.offset_sma.is_none());
                    debug_assert!(meta.offset_dict.is_none());
                    debug_assert!(meta.offset_str.is_none());
                    // load single value from data
                    // 4-byte length, 4-byte number of tuples.
                    let offset_data = meta.offset_data.get() as usize;
                    let len = u32::from_ne_bytes(
                        self.inner[offset_data + SG_OFFSET_START_BYTES
                            ..offset_data + SG_OFFSET_END_BYTES]
                            .try_into()?,
                    );
                    let n_values = u32::from_ne_bytes(
                        self.inner[offset_data + SG_OFFSET_START_VALUES
                            ..offset_data + SG_OFFSET_END_VALUES]
                            .try_into()?,
                    );
                    debug_assert!(n_values as u16 == self.n_records());
                    if len == u32::MAX {
                        // NULL
                        let codec = Codec::Single(SingleCodec::new_null(n_values as usize));
                        Ok(Attr {
                            ty: meta.ty,
                            codec,
                            psma: None,
                        })
                    } else {
                        let mut data: SmallVec<[u8; 16]> = smallvec![];
                        data.extend_from_slice(
                            &self.inner[offset_data + 8..offset_data + 8 + len as usize],
                        );
                        let codec =
                            Codec::Single(SingleCodec::raw_from_bytes(data, n_values as usize));
                        Ok(Attr {
                            ty: meta.ty,
                            codec,
                            psma: None,
                        })
                    }
                }
                CompressMethod::Flat => todo!(),
            })
    }
}

#[derive(Debug, Clone)]
pub struct AttrMeta {
    pub ty: PreciseType,
    pub cmprs_mthd: CompressMethod,
    pub offset_sma: Option<NonZeroU64>,
    pub offset_dict: Option<NonZeroU64>,
    pub offset_data: NonZeroU64,
    pub offset_str: Option<NonZeroU64>,
}

impl TryFrom<&[u8]> for AttrMeta {
    type Error = Error;
    #[inline]
    fn try_from(src: &[u8]) -> Result<Self> {
        let ty_bytes: [u8; 8] =
            src[ATTR_META_OFFSET_START_TY..ATTR_META_OFFSET_END_TY].try_into()?;
        let ty = PreciseType::try_from(ty_bytes).map_err(|_| Error::InvalidFormat)?;
        let cmprs_mthd = CompressMethod::try_from(
            &src[ATTR_META_OFFSET_START_CMPRS_MTHD..ATTR_META_OFFSET_END_CMPRS_MTHD],
        )?;
        let offset_sma = u64::from_ne_bytes(
            src[ATTR_META_OFFSET_START_SMA..ATTR_META_OFFSET_END_SMA].try_into()?,
        );
        let offset_sma = NonZeroU64::new(offset_sma);
        let offset_dict = u64::from_ne_bytes(
            src[ATTR_META_OFFSET_START_DICT..ATTR_META_OFFSET_END_DICT].try_into()?,
        );
        let offset_dict = NonZeroU64::new(offset_dict);
        let offset_data = u64::from_ne_bytes(
            src[ATTR_META_OFFSET_START_DATA..ATTR_META_OFFSET_END_DATA].try_into()?,
        );
        let offset_data = NonZeroU64::new(offset_data).ok_or(Error::InvalidFormat)?;
        let offset_str = u64::from_ne_bytes(
            src[ATTR_META_OFFSET_START_STR..ATTR_META_OFFSET_END_STR].try_into()?,
        );
        let offset_str = NonZeroU64::new(offset_str);
        Ok(AttrMeta {
            ty,
            cmprs_mthd,
            offset_sma,
            offset_dict,
            offset_data,
            offset_str,
        })
    }
}

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CompressMethod {
    Single = 0,
    Flat = 1,
    // Trunc1B = 2,
    // Trunc2B = 3,
    // Trunc4B = 4,
    // Dict1B = 5,
    // Dict2B = 6,
    // Dict4B = 7,
}

impl TryFrom<&[u8]> for CompressMethod {
    type Error = Error;
    #[inline]
    fn try_from(src: &[u8]) -> Result<Self> {
        use CompressMethod::*;
        let v = u16::from_ne_bytes(src.try_into().unwrap());
        let res = match v {
            0 => Single,
            1 => Flat,
            // 2 => Trunc1B,
            // 3 => Trunc2B,
            // 4 => Trunc4B,
            // 5 => Dict1B,
            // 6 => Dict2B,
            // 7 => Dict4B,
            _ => return Err(Error::InvalidFormat),
        };
        Ok(res)
    }
}

impl From<CompressMethod> for [u8; 2] {
    #[inline]
    fn from(src: CompressMethod) -> Self {
        (src as u16).to_ne_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};
    #[test]
    fn test_block_single_codec_persist() {
        let codec1 = Codec::Single(SingleCodec::new_null(1024));
        let attr1 = Attr {
            ty: PreciseType::i32(),
            codec: codec1,
            psma: None,
        };
        let codec2 = Codec::Single(SingleCodec::new(1i32, 1024));
        let attr2 = Attr {
            ty: PreciseType::i32(),
            codec: codec2,
            psma: None,
        };
        let block = Block {
            len: 1024,
            attrs: vec![attr1, attr2],
        };
        let mut tmpfile: File = tempfile::tempfile().unwrap();
        let mut buf = Vec::with_capacity(4096);
        let write_len = block.persist(&mut tmpfile, &mut buf).unwrap();
        tmpfile.seek(SeekFrom::Start(0)).unwrap();
        // buf.clear();
        let mut raw_block = Vec::with_capacity(write_len);
        let read_len = tmpfile.read_to_end(&mut raw_block).unwrap();
        assert_eq!(write_len, read_len);
        let raw_block = RawBlock::new(Arc::from(raw_block.into_boxed_slice()));
        assert_eq!(read_len, raw_block.len());
        assert_eq!(1024, raw_block.n_records());
        assert_eq!(2, raw_block.n_attrs());
        let attr1_new = raw_block.attr(0).unwrap();
        assert_eq!(PreciseType::i32(), attr1_new.ty);
        assert!(attr1_new.psma.is_none());
        if let Codec::Single(s) = attr1_new.codec {
            assert!(!s.is_valid());
        } else {
            panic!("failed")
        }
        let attr2_new = raw_block.attr(1).unwrap();
        assert_eq!(PreciseType::i32(), attr2_new.ty);
        if let Codec::Single(s) = attr2_new.codec {
            let (valid, value) = s.view::<i32>();
            assert!(valid);
            assert_eq!(1i32, value);
        } else {
            panic!("failed")
        }
    }
}
