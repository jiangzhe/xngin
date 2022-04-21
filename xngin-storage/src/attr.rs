use crate::block::CompressMethod;
use crate::codec::{Codec, Single};
use crate::error::{Error, Result};
use std::num::NonZeroU64;
use std::sync::Arc;
use xngin_common::array::Array;
use xngin_common::bitmap::Bitmap;
use xngin_common::byte_repr::ByteRepr;
use xngin_common::sma::SMA;
use xngin_datatype::{PreciseType, StaticTyped};

// attribute header level offset
const ATTR_HDR_OFFSET_START_TY: usize = 0;
const ATTR_HDR_OFFSET_END_TY: usize = 8;
const ATTR_HDR_OFFSET_START_CMPRS_MTHD: usize = 8;
const ATTR_HDR_OFFSET_END_CMPRS_MTHD: usize = 10;
const ATTR_HDR_OFFSET_START_SMA: usize = 16;
const ATTR_HDR_OFFSET_END_SMA: usize = 24;
const ATTR_HDR_OFFSET_START_DICT: usize = 24;
const ATTR_HDR_OFFSET_END_DICT: usize = 32;
const ATTR_HDR_OFFSET_START_DATA: usize = 32;
const ATTR_HDR_OFFSET_END_DATA: usize = 40;
const ATTR_HDR_OFFSET_START_STR: usize = 40;
const ATTR_HDR_OFFSET_END_STR: usize = 48;
pub(crate) const LEN_ATTR_HDR: usize = 48;

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
///
/// Flat Codec(Non-string): Fixed-length array.
#[derive(Debug)]
pub struct Attr {
    pub ty: PreciseType,
    pub validity: Option<Arc<Bitmap>>,
    pub codec: Codec,
    pub sma: Option<SMA>,
}

impl Attr {
    /// Create new attribute with single codec.
    #[inline]
    pub fn new_single(ty: PreciseType, single: Single) -> Attr {
        Attr {
            ty,
            validity: None,
            codec: Codec::Single(single),
            sma: None,
        }
    }

    /// Convert self to owned.
    #[inline]
    pub fn to_owned(&self) -> Self {
        Attr {
            ty: self.ty,
            validity: self.validity.as_ref().map(Bitmap::clone_to_owned),
            codec: Codec::to_owned(&self.codec),
            sma: None,
        }
    }

    /// Returns number of records.
    #[inline]
    pub fn n_records(&self) -> usize {
        self.codec.n_records()
    }

    // #[inline]
    // pub fn calc_meta(&self, offset: u64) -> (AttrHeader, u64) {
    //     let data_len = self.codec.ser_len() as u64;
    //     match &self.codec {
    //         Codec::Single(_) => {
    //             assert!(self.sma.is_none());
    //             let offset_data = NonZeroU64::new(offset).unwrap();
    //             let meta = AttrHeader {
    //                 ty: self.ty,
    //                 cmprs_mthd: CompressMethod::Single,
    //                 offset_sma: None,
    //                 offset_dict: None,
    //                 offset_data,
    //                 offset_str: None,
    //             };
    //             (meta, align_u128(offset + data_len))
    //         }
    //         Codec::Flat(_) => {
    //             // let offset_data = NonZeroU64::new(offset).unwrap();
    //             let sma_len = self.sma.as_ref().map(|sma| sma.ser_len() as u64);
    //             // todo: dict_len
    //             // todo: str_len
    //             let offset_sma = sma_len.and_then(|_| NonZeroU64::new(offset));
    //             let offset_data = NonZeroU64::new(offset + sma_len.unwrap_or(0)).unwrap();
    //             let meta = AttrHeader {
    //                 ty: self.ty,
    //                 cmprs_mthd: CompressMethod::Flat,
    //                 offset_sma,
    //                 offset_dict: None,
    //                 offset_data,
    //                 offset_str: None,
    //             };
    //             (meta, align_u128(offset_data.get() + data_len))
    //         }
    //     }
    // }

    // #[inline]
    // pub fn store<W: io::Write>(&self, writer: &mut W, buf: &mut Vec<u8>) -> io::Result<usize> {
    //     match &self.codec {
    //         Codec::Single(s) => {
    //             buf.clear();
    //             // number of bytes
    //             // if null, set to u32::MAX
    //             let n_bytes = if !s.is_valid() {
    //                 u32::MAX
    //             } else {
    //                 s.data_len() as u32
    //             };
    //             buf.extend_from_slice(&n_bytes.to_ne_bytes());
    //             buf.extend_from_slice(&(s.len() as u32).to_ne_bytes());
    //             buf.extend_from_slice(s.raw_data());
    //             let buf_len = buf.len() as u64;
    //             for _ in buf_len..align_u128(buf_len) {
    //                 buf.push(0);
    //             }
    //             writer.write_all(buf)?;
    //             Ok(buf.len())
    //         }
    //         Codec::Flat(f) => {
    //             match self.ty {
    //                 PreciseType::Int(4, _) => {
    //                     // let (vm, i32s) = f.view::<i32>();
    //                     let mut write_bytes = 0;
    //                     if let Some(vm) = f.validity.as_ref() {
    //                         write_bytes += u64::write_all(writer, vm.u64s_occupied().0)?;
    //                     }
    //                     write_bytes += i32::write_all(writer, f.data.cast_slice::<i32>())?;
    //                     let total_bytes = align_u128(write_bytes as u64);
    //                     if total_bytes as usize > write_bytes {
    //                         writer.write_all(&vec![0u8; total_bytes as usize - write_bytes])?;
    //                     }
    //                     Ok(total_bytes as usize)
    //                 }
    //                 _ => todo!(),
    //             }
    //         }
    //     }
    // }
}

impl<T: ByteRepr + StaticTyped + Default> FromIterator<Option<T>> for Attr {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let iter_size = match iter.size_hint() {
            (_, Some(hb)) => hb.max(64),
            _ => 64,
        };
        let mut validity = Bitmap::with_capacity(iter_size);
        let mut validity_u64s = validity.reserve_u64s(iter_size);
        let mut data = Array::new_owned::<T>(iter_size);
        let mut data_slice = data.cast_slice_mut::<T>(iter_size).unwrap();

        let mut len = 0usize;
        let mut buffer = Vec::with_capacity(64);
        let mut bitmask = 1u64;
        let mut word = 0u64;
        for item in iter {
            if let Some(v) = item {
                word |= bitmask;
                buffer.push(v);
            } else {
                buffer.push(T::default());
            }
            len += 1;
            if len & 63 == 0 {
                // add to validity
                let vidx = len / 64 - 1;
                if vidx == validity_u64s.len() {
                    validity_u64s = validity.reserve_u64s(vidx * 2 * 64);
                }
                validity_u64s[vidx] = word;
                word = 0;
                bitmask = 1;
                // add to data
                if len >= data_slice.len() {
                    let new_len = len.max(data_slice.len() * 2);
                    data_slice = data.cast_slice_mut::<T>(new_len).unwrap();
                }
                data_slice[len - 64..len].clone_from_slice(&buffer);
                buffer.clear();
            } else {
                bitmask <<= 1;
            }
        }
        if !buffer.is_empty() {
            // add to validity
            let vidx = len / 64;
            if vidx == validity_u64s.len() {
                validity_u64s = validity.reserve_u64s(vidx * 2 * 64);
            }
            validity_u64s[vidx] = word;
            // add to data
            if len >= data_slice.len() {
                let new_len = len.max(data_slice.len() * 2);
                data_slice = data.cast_slice_mut::<T>(new_len).unwrap();
            }
            data_slice[len & !63..len].clone_from_slice(&buffer);
            buffer.clear();
        }
        // update length
        unsafe { data.set_len(len) };
        unsafe { validity.set_len(len) };
        let codec = Codec::new_array(data);
        Attr {
            ty: T::static_pty(),
            validity: Some(Arc::new(validity)),
            sma: None,
            codec,
        }
    }
}

impl<T, I> From<I> for Attr
where
    T: ByteRepr + StaticTyped,
    I: ExactSizeIterator<Item = T>,
{
    #[inline]
    fn from(src: I) -> Self {
        let len = src.len();
        let mut data = Array::new_owned::<T>(len);
        let data_slice = data.cast_slice_mut::<T>(len).unwrap();
        for (t, s) in data_slice.iter_mut().zip(src) {
            *t = s;
        }
        unsafe { data.set_len(len) };
        let codec = Codec::new_array(data);
        Attr {
            ty: T::static_pty(),
            validity: None,
            sma: None,
            codec,
        }
    }
}

/// Attribute header:
/// 1. Attribute precise type: 8B.
/// 2. Compression method: 2B with 6B padding.
/// 3. Offset SMA: 8B.
/// 4. Offset dict: 8B. 0 means there is no dict.
/// 5. Offset data: 8B.
/// 6. Offset string: 8B. 0 means there is no string.
///
/// All offsets listed above start from first byte this block.
/// Attribute header has length 40B in total.
#[derive(Debug, Clone)]
pub struct AttrHeader {
    pub ty: PreciseType,
    pub cmprs_mthd: CompressMethod,
    pub offset_sma: Option<NonZeroU64>,
    pub offset_dict: Option<NonZeroU64>,
    pub offset_data: NonZeroU64,
    pub offset_str: Option<NonZeroU64>,
}

impl TryFrom<&[u8]> for AttrHeader {
    type Error = Error;
    #[inline]
    fn try_from(src: &[u8]) -> Result<Self> {
        let ty_bytes: [u8; 8] = src[ATTR_HDR_OFFSET_START_TY..ATTR_HDR_OFFSET_END_TY].try_into()?;
        let ty = PreciseType::try_from(ty_bytes).map_err(|_| Error::InvalidFormat)?;
        let cmprs_mthd = CompressMethod::try_from(
            &src[ATTR_HDR_OFFSET_START_CMPRS_MTHD..ATTR_HDR_OFFSET_END_CMPRS_MTHD],
        )?;
        let offset_sma =
            u64::from_ne_bytes(src[ATTR_HDR_OFFSET_START_SMA..ATTR_HDR_OFFSET_END_SMA].try_into()?);
        let offset_sma = NonZeroU64::new(offset_sma);
        let offset_dict = u64::from_ne_bytes(
            src[ATTR_HDR_OFFSET_START_DICT..ATTR_HDR_OFFSET_END_DICT].try_into()?,
        );
        let offset_dict = NonZeroU64::new(offset_dict);
        let offset_data = u64::from_ne_bytes(
            src[ATTR_HDR_OFFSET_START_DATA..ATTR_HDR_OFFSET_END_DATA].try_into()?,
        );
        let offset_data = NonZeroU64::new(offset_data).ok_or(Error::InvalidFormat)?;
        let offset_str =
            u64::from_ne_bytes(src[ATTR_HDR_OFFSET_START_STR..ATTR_HDR_OFFSET_END_STR].try_into()?);
        let offset_str = NonZeroU64::new(offset_str);
        Ok(AttrHeader {
            ty,
            cmprs_mthd,
            offset_sma,
            offset_dict,
            offset_data,
            offset_str,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_array_codec_from_iter() {
        let attr = Attr::from_iter(vec![Some(1i32), None, Some(3)]);
        let bm = attr.validity.as_ref().unwrap();
        assert!(bm.get(0).unwrap());
        assert!(!bm.get(1).unwrap());
        assert!(bm.get(2).unwrap());
        assert_eq!(
            &[1, 0, 3],
            attr.codec.as_array().unwrap().cast_slice::<i32>()
        );
    }
}
