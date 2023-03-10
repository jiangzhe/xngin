mod single;

use std::sync::Arc;

use crate::array::Array;
use crate::bitmap::Bitmap;
pub use single::Single;

/// Codec of attributes.
/// The design of codec is heavily inspired by paper "Data Blocks: Hybrid OLTP
/// and OLAP on Compressed Storage using both Vectorization and Compilation".
/// The main idea is to make the codec byte-addressable, similar between
/// in-memory and persistent format.
/// Byte-addressable is to make the point search efficient, especially without
/// decompression.
/// the similar format between memory and disk enable easy persistence.
#[derive(Debug, Clone)]
pub enum Codec {
    /// One distinct value.
    /// There are two scenarios:
    /// 1. All values in one block are same.
    /// 2. Only one value in intermediate block, which is common when
    ///    filter expression is evaluated to single row. e.g. primary key
    ///    search in OLTP.
    Single(Single),
    /// Array codec stores values in contiguous memory area.
    /// It enables SIMD instructions upon such values.
    Array(Arc<Array>),
    /// Bitmap codec encodes bool values in bitmap.
    Bitmap(Arc<Bitmap>),
    /// Empty means there is no values.
    Empty,
}

impl Codec {
    /// Create a new array codec.
    #[inline]
    pub fn new_array(data: Array) -> Self {
        Codec::Array(Arc::new(data))
    }

    /// Create a new bitmap codec.
    #[inline]
    pub fn new_bitmap(data: Bitmap) -> Self {
        Codec::Bitmap(Arc::new(data))
    }

    #[inline]
    pub fn empty() -> Self {
        Codec::Empty
    }

    /// Clone self to owned.
    #[inline]
    pub fn to_owned(&self) -> Self {
        match self {
            Codec::Single(s) => Codec::Single(s.clone()),
            Codec::Array(a) => Codec::Array(Arc::new(Array::to_owned(&**a))),
            Codec::Bitmap(b) => Codec::Bitmap(Arc::new(Bitmap::to_owned(b))),
            Codec::Empty => Codec::Empty,
        }
    }

    /// Returns number of records.
    #[inline]
    pub fn n_records(&self) -> usize {
        match self {
            Codec::Single(s) => s.len as usize,
            Codec::Array(a) => a.len(),
            Codec::Bitmap(b) => b.len(),
            Codec::Empty => 0,
        }
    }

    /// Returns array codec if match.
    #[inline]
    pub fn as_array(&self) -> Option<&Array> {
        match self {
            Codec::Array(a) => Some(a.as_ref()),
            _ => None,
        }
    }

    /// Returns single codec if match.
    #[inline]
    pub fn as_single(&self) -> Option<&Single> {
        match self {
            Codec::Single(s) => Some(s),
            _ => None,
        }
    }

    /// Returns bitmap codec if match.
    #[inline]
    pub fn as_bitmap(&self) -> Option<&Bitmap> {
        match self {
            Codec::Bitmap(b) => Some(b),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecKind {
    Single,
    Array,
    // Dict,
    // Trunc,
    // String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_codec_common_ops() {
        let c = Codec::Single(Single::new_null(1024));
        assert!(c.as_single().is_some());
        assert_eq!(1024, c.n_records());
        let c = Codec::new_array(Array::from(0i32..1024));
        assert!(c.as_array().is_some());
        assert_eq!(1024, c.n_records());
        let c = Codec::new_bitmap(Bitmap::from_iter(vec![true; 1024]));
        assert!(c.as_bitmap().is_some());
        assert_eq!(1024, c.n_records());
    }
}
