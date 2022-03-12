use xngin_common::array::{ArrayCast, VecArray, ViewArray};
use xngin_common::bitmap::{ReadBitmap, VecBitmap, ViewBitmap};

/// Codec of attributes.
/// The design of codec is heavily inspired by paper "Data Blocks: Hybrid OLTP
/// and OLAP on Compressed Storage using both Vectorization and Compilation".
/// The main idea is to make the codec byte-addressable, consistent with
/// in-memory and persistent format.
/// Byte-addressable is to make the point search efficient, especially without
/// decompression.
/// the consistency between memory and disk requires little effort on serialization
/// and deserialization.
pub enum Codec {
    Single(SingleCodec),
    Flat(FlatCodec),
}

impl Codec {
    #[inline]
    pub fn kind(&self) -> CodecKind {
        match self {
            Codec::Single(_) => CodecKind::Single,
            Codec::Flat(_) => CodecKind::Flat,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecKind {
    Single,
    Flat,
    // Dict,
    // Trunc,
    // String,
}

/// Single codec encodes single value as bytes in case all
/// values of that attribute are identical.
pub struct SingleCodec {
    valid: bool,
    data: Vec<u8>,
}

impl SingleCodec {
    #[inline]
    pub fn view_i32(&self) -> (bool, i32) {
        if self.valid {
            let mut v = [0u8; 4];
            v.copy_from_slice(&self.data[..4]);
            (true, i32::from_ne_bytes(v))
        } else {
            (false, 0)
        }
    }

    #[inline]
    pub fn new_null() -> Self {
        SingleCodec {
            valid: false,
            data: vec![],
        }
    }

    #[inline]
    pub fn new_i32(val: i32) -> Self {
        let mut data = vec![0u8; 4];
        data.copy_from_slice(&val.to_ne_bytes());
        SingleCodec { valid: true, data }
    }
}

pub enum FlatCodec {
    Borrowed(BorrowFlat),
    Owned(OwnFlat),
}

impl FlatCodec {
    #[inline]
    pub fn new_owned(validity: Option<VecBitmap>, data: VecArray) -> Self {
        FlatCodec::Owned(OwnFlat { validity, data })
    }

    #[inline]
    pub fn view_i32s(&self) -> (Option<&[u8]>, &[i32]) {
        match self {
            FlatCodec::Borrowed(bf) => bf.view_i32s(),
            FlatCodec::Owned(of) => of.view_i32s(),
        }
    }
}

/// Flat codec represents uncompressed data of one attribute.
/// Ususally the data type is not
pub struct BorrowFlat {
    validity: Option<ViewBitmap>,
    data: ViewArray,
}

impl BorrowFlat {
    #[inline]
    pub fn view_i32s(&self) -> (Option<&[u8]>, &[i32]) {
        let valid_map = self.validity.as_ref().map(|vm| vm.aligned().0);
        let data = self.data.cast_i32s();
        (valid_map, data)
    }
}

pub struct OwnFlat {
    validity: Option<VecBitmap>,
    data: VecArray,
}

impl OwnFlat {
    #[inline]
    pub fn view_i32s(&self) -> (Option<&[u8]>, &[i32]) {
        let valid_map = self.validity.as_ref().map(|vm| vm.aligned().0);
        let data = self.data.cast_i32s();
        (valid_map, data)
    }
}

impl FromIterator<i32> for OwnFlat {
    #[inline]
    fn from_iter<T: IntoIterator<Item = i32>>(iter: T) -> Self {
        let i32s: Vec<_> = iter.into_iter().collect();
        let data = VecArray::from(i32s);
        OwnFlat {
            validity: None,
            data,
        }
    }
}
