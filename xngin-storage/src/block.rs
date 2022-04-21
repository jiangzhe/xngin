use crate::attr::{Attr, AttrHeader, LEN_ATTR_HDR};
use crate::codec::{Codec, Single};
use crate::error::{Error, Result};
use smallvec::{smallvec, SmallVec};
use std::sync::Arc;

/// Block collects multiple tuples and aggregate synopses for analytical query.
/// On-disk format is described as below.
///
/// Block:
/// 1. Block header.
/// 2. Block data.
pub struct Block {
    pub data: BlockData,
    header: Option<BlockHeader>,
}

impl Block {
    /// Create a new block with given data and header.
    #[inline]
    pub fn with_header(data: BlockData, header: BlockHeader) -> Self {
        Block {
            data,
            header: Some(header),
        }
    }

    /// Create a new block with given data.
    #[inline]
    pub fn new(data: BlockData) -> Self {
        Block { data, header: None }
    }

    /// Returns number of records.
    #[inline]
    pub fn n_records(&self) -> usize {
        if let Some(header) = self.header.as_ref() {
            return header.n_records as usize;
        }
        self.data[0].n_records()
    }

    /// make header if not exists.
    #[inline]
    pub fn make_header(&mut self) -> &BlockHeader {
        self.header
            .get_or_insert_with(|| make_block_header(&self.data))
    }

    /// Returns header if exists.
    #[inline]
    pub fn header(&self) -> Option<&BlockHeader> {
        self.header.as_ref()
    }
}

#[inline]
pub fn make_block_header(data: &[Attr]) -> BlockHeader {
    todo!()
}

/// Block Header:
/// 1. Number of records: 2B with 2B padding.
/// 2. Number of attributes: 2B with 2B padding.
/// 3. Reserved padding: 8B.
/// 4. Attribute header, ..., Attribute header: N * 48 B.
pub struct BlockHeader {
    pub n_records: u16,
    pub padding: [u8; 8],
    pub attr_headers: Vec<AttrHeader>,
}

/// Block Data:
/// 1. Attribute data, ..., Attribute data: variable length.
pub type BlockData = Vec<Attr>;

impl Block {
    #[inline]
    pub fn fetch_attr(&self, ext_idx: usize) -> Option<Attr> {
        self.data.get(ext_idx).map(Attr::to_owned)
    }

    // #[inline]
    // pub fn store<W: io::Write>(&self, writer: &mut W, buf: &mut Vec<u8>) -> io::Result<usize> {
    //     buf.clear();
    //     // number of records
    //     buf.extend_from_slice(&u16::to_ne_bytes(self.len as u16));
    //     buf.extend_from_slice(&[0u8; 6]); // padding
    //                                       // number of attributes
    //     buf.extend_from_slice(&u16::to_ne_bytes(self.attrs.len() as u16));
    //     buf.extend_from_slice(&[0u8; 6]); // padding
    //                                       // preserve metadata offset
    //     let mut offset = 16u64 + (self.attrs.len() * LEN_ATTR_META) as u64;
    //     let mut expected_lens = Vec::with_capacity(self.attrs.len());
    //     for attr in &self.attrs {
    //         let (meta, new_offset) = attr.calc_meta(offset);
    //         // precise type
    //         buf.extend_from_slice(&<[u8; 8]>::from(meta.ty));
    //         // compression method
    //         buf.extend_from_slice(&<[u8; 2]>::from(meta.cmprs_mthd));
    //         buf.extend_from_slice(&[0u8; 6]); // padding
    //                                           // offset sma
    //         buf.extend_from_slice(
    //             &meta
    //                 .offset_sma
    //                 .map(|v| v.get())
    //                 .unwrap_or_default()
    //                 .to_ne_bytes(),
    //         );
    //         // offset dict
    //         buf.extend_from_slice(
    //             &meta
    //                 .offset_dict
    //                 .map(|v| v.get())
    //                 .unwrap_or_default()
    //                 .to_ne_bytes(),
    //         );
    //         // offset data
    //         buf.extend_from_slice(&meta.offset_data.get().to_ne_bytes());
    //         // offset str
    //         buf.extend_from_slice(
    //             &meta
    //                 .offset_str
    //                 .map(|v| v.get())
    //                 .unwrap_or_default()
    //                 .to_ne_bytes(),
    //         );
    //         expected_lens.push(new_offset - offset);
    //         offset = new_offset;
    //     }
    //     // persist metadata
    //     writer.write_all(buf)?;
    //     buf.clear();
    //     // persist each attribute
    //     for (attr, expected_len) in self.attrs.iter().zip(expected_lens) {
    //         let len = attr.store(writer, buf)?;
    //         assert_eq!(expected_len as usize, len);
    //     }
    //     writer.flush()?;
    //     Ok(offset as usize)
    // }
}

// block level offset
const OFFSET_START_N_RECORDS: usize = 0;
const OFFSET_END_N_RECORDS: usize = 2;
const OFFSET_START_N_ATTRS: usize = 8;
const OFFSET_END_N_ATTRS: usize = 10;
const OFFSET_START_ATTR_META: usize = 16;

// single codec offset
const SG_OFFSET_START_BYTES: usize = 0;
const SG_OFFSET_END_BYTES: usize = 4;
const SG_OFFSET_START_VALUES: usize = 4;
const SG_OFFSET_END_VALUES: usize = 8;

/// RawBlock is the serialized byte format of Block, which could
/// be written directly to disk, and could be also read
/// directly from disk.
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

    /// Returns attribute header.
    #[inline]
    fn attr_header(&self, attr_id: usize) -> Option<AttrHeader> {
        if attr_id >= self.n_attrs() as usize {
            return None;
        }
        let start = OFFSET_START_ATTR_META + attr_id * LEN_ATTR_HDR;
        let end = start + LEN_ATTR_HDR;
        let attr_header = AttrHeader::try_from(&self.inner[start..end]).unwrap();
        Some(attr_header)
    }

    #[inline]
    pub fn attr(&self, attr_id: usize) -> Result<Attr> {
        self.attr_header(attr_id)
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
                        let codec = Codec::Single(Single::new_null(n_values as usize));
                        Ok(Attr {
                            ty: meta.ty,
                            validity: None,
                            codec,
                            sma: None,
                        })
                    } else {
                        let mut data: SmallVec<[u8; 16]> = smallvec![];
                        data.extend_from_slice(
                            &self.inner[offset_data + 8..offset_data + 8 + len as usize],
                        );
                        let codec = Codec::Single(Single::raw_from_bytes(data, n_values as usize));
                        Ok(Attr {
                            ty: meta.ty,
                            validity: None,
                            codec,
                            sma: None,
                        })
                    }
                }
                CompressMethod::Flat => {
                    if meta.offset_sma.is_some() {
                        todo!("load sma")
                    }
                    if meta.offset_dict.is_some() {
                        todo!("load dict")
                    }
                    // let inner = Arc::clone(&self.inner);
                    // match meta.ty {
                    //     PreciseType::Int(4, true) => {
                    //         Codec::Flat(FlatCodec::)
                    //     }
                    // }
                    todo!()
                }
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
    use crate::block::{Attr, RawBlock};
    use crate::codec::{Codec, Single};
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::Arc;
    use xngin_datatype::PreciseType;

    // #[test]
    // fn test_block_single_codec_store() {
    //     let codec1 = Codec::Single(Single::new_null(1024));
    //     let attr1 = Attr {
    //         ty: PreciseType::i32(),
    //         validity: None,
    //         codec: codec1,
    //         sma: None,
    //     };
    //     let codec2 = Codec::Single(Single::new(1i32, 1024));
    //     let attr2 = Attr {
    //         ty: PreciseType::i32(),
    //         validity: None,
    //         codec: codec2,
    //         sma: None,
    //     };
    //     let block = Block {
    //         len: 1024,
    //         attrs: vec![attr1, attr2],
    //     };
    //     let mut tmpfile: File = tempfile::tempfile().unwrap();
    //     let mut buf = Vec::with_capacity(4096);
    //     let write_len = block.store(&mut tmpfile, &mut buf).unwrap();
    //     tmpfile.seek(SeekFrom::Start(0)).unwrap();
    //     // buf.clear();
    //     let mut raw_block = Vec::with_capacity(write_len);
    //     let read_len = tmpfile.read_to_end(&mut raw_block).unwrap();
    //     assert_eq!(write_len, read_len);
    //     let raw_block = RawBlock::new(Arc::from(raw_block.into_boxed_slice()));
    //     assert_eq!(read_len, raw_block.len());
    //     assert_eq!(1024, raw_block.n_records());
    //     assert_eq!(2, raw_block.n_attrs());
    //     let attr1_new = raw_block.attr(0).unwrap();
    //     assert_eq!(PreciseType::i32(), attr1_new.ty);
    //     assert!(attr1_new.sma.is_none());
    //     if let Codec::Single(s) = attr1_new.codec {
    //         assert!(!s.is_valid());
    //     } else {
    //         panic!("failed")
    //     }
    //     let attr2_new = raw_block.attr(1).unwrap();
    //     assert_eq!(PreciseType::i32(), attr2_new.ty);
    //     if let Codec::Single(s) = attr2_new.codec {
    //         let (valid, value) = s.view::<i32>();
    //         assert!(valid);
    //         assert_eq!(1i32, value);
    //     } else {
    //         panic!("failed")
    //     }
    // }

    // #[test]
    // fn test_block_flat_codec_store() {
    //     let codec1 = Codec::new_flat(FlatCodec::from(0i32..1024));
    //     let attr1 = Attr {
    //         ty: PreciseType::i32(),
    //         codec: codec1,
    //         sma: None,
    //     };
    //     let block = Block {
    //         len: 1024,
    //         attrs: vec![attr1],
    //     };
    //     let mut tmpfile: File = tempfile::tempfile().unwrap();
    //     let mut buf = Vec::with_capacity(4096);
    //     let write_len = block.store(&mut tmpfile, &mut buf).unwrap();
    //     tmpfile.seek(SeekFrom::Start(0)).unwrap();
    //     // buf.clear();
    //     let mut raw_block = Vec::with_capacity(write_len);
    //     let read_len = tmpfile.read_to_end(&mut raw_block).unwrap();
    //     let raw_block = RawBlock::new(Arc::from(raw_block.into_boxed_slice()));
    //     assert_eq!(read_len, raw_block.len());
    //     assert_eq!(write_len, read_len);
    //     assert_eq!(1024, raw_block.n_records());
    //     assert_eq!(1, raw_block.n_attrs());
    //     let attr1_new = raw_block.attr(0).unwrap();
    //     assert_eq!(PreciseType::i32(), attr1_new.ty);
    //     assert!(attr1_new.sma.is_none());
    // }
}
