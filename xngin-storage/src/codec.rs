use smallvec::{smallvec, SmallVec};
use xngin_common::array::{ArrayCast, VecArray, ViewArray};
use xngin_common::bitmap::{AppendBitmap, ReadBitmap, VecBitmap, ViewBitmap};
use xngin_common::byte_repr::ByteRepr;

/// Codec of attributes.
/// The design of codec is heavily inspired by paper "Data Blocks: Hybrid OLTP
/// and OLAP on Compressed Storage using both Vectorization and Compilation".
/// The main idea is to make the codec byte-addressable, similar between
/// in-memory and persistent format.
/// Byte-addressable is to make the point search efficient, especially without
/// decompression.
/// the similar format between memory and disk enable easy persistence.
pub enum Codec {
    Single(SingleCodec),
    Flat(FlatCodec),
}

#[allow(clippy::len_without_is_empty)]
impl Codec {
    #[inline]
    pub fn kind(&self) -> CodecKind {
        match self {
            Codec::Single(_) => CodecKind::Single,
            Codec::Flat(_) => CodecKind::Flat,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Codec::Single(s) => s.len(),
            Codec::Flat(f) => f.len(),
        }
    }
}

impl FromIterator<Option<i32>> for Codec {
    #[inline]
    fn from_iter<T: IntoIterator<Item = Option<i32>>>(iter: T) -> Self {
        let mut bm = VecBitmap::new();
        let mut data = vec![];
        for item in iter {
            match item {
                Some(v) => {
                    bm.add(true).unwrap();
                    data.push(v);
                }
                None => {
                    bm.add(false).unwrap();
                    data.push(0);
                }
            }
        }
        assert!(data.len() < u16::MAX as usize);
        let true_count = bm.true_count();
        if true_count == 0 {
            // that means all values are null, we create single codec
            return Codec::Single(SingleCodec::new_null(data.len()));
        }
        if true_count == data.len() {
            // that means all values are not null, we discard validity map
            return Codec::Flat(FlatCodec::Owned(OwnFlat::new(None, VecArray::from(data))));
        }
        Codec::Flat(FlatCodec::Owned(OwnFlat::new(
            Some(bm),
            VecArray::from(data),
        )))
    }
}

impl From<Vec<i32>> for Codec {
    #[inline]
    fn from(src: Vec<i32>) -> Self {
        Codec::Flat(FlatCodec::Owned(OwnFlat::new(None, VecArray::from(src))))
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
    data: SmallVec<[u8; 16]>,
    len: usize,
}

#[allow(clippy::len_without_is_empty)]
impl SingleCodec {
    #[inline]
    pub fn view<T: ByteRepr>(&self) -> (bool, T) {
        if self.valid {
            (true, T::from_bytes(&self.data))
        } else {
            (false, T::default())
        }
    }

    #[inline]
    pub fn new_null(len: usize) -> Self {
        SingleCodec {
            valid: false,
            data: smallvec![],
            len,
        }
    }

    #[inline]
    pub fn new<T: ByteRepr>(val: T, len: usize) -> Self {
        SingleCodec {
            valid: true,
            data: val.to_bytes(),
            len,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }
}

pub enum FlatCodec {
    Borrowed(BorrowFlat),
    Owned(OwnFlat),
}

#[allow(clippy::len_without_is_empty)]
impl FlatCodec {
    #[inline]
    pub fn new_owned(validity: Option<VecBitmap>, data: VecArray) -> Self {
        FlatCodec::Owned(OwnFlat { validity, data })
    }

    #[inline]
    pub fn view<T: ByteRepr>(&self) -> (Option<&[u8]>, &[T]) {
        match self {
            FlatCodec::Borrowed(bf) => bf.view(),
            FlatCodec::Owned(of) => of.view(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            FlatCodec::Borrowed(bf) => bf.len(),
            FlatCodec::Owned(of) => of.len(),
        }
    }
}

/// Flat codec represents uncompressed data of one attribute.
pub struct BorrowFlat {
    validity: Option<ViewBitmap>,
    data: ViewArray,
}

#[allow(clippy::len_without_is_empty)]
impl BorrowFlat {
    #[inline]
    pub fn view<T: ByteRepr>(&self) -> (Option<&[u8]>, &[T]) {
        let valid_map = self.validity.as_ref().map(|vm| vm.aligned().0);
        let data = self.data.cast();
        (valid_map, data)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

pub struct OwnFlat {
    validity: Option<VecBitmap>,
    data: VecArray,
}

#[allow(clippy::len_without_is_empty)]
impl OwnFlat {
    #[inline]
    pub fn new(validity: Option<VecBitmap>, data: VecArray) -> Self {
        if let Some(bm) = validity.as_ref() {
            assert!(bm.len() == data.len());
        }
        OwnFlat::new(validity, data)
    }

    #[inline]
    pub fn view<T: ByteRepr>(&self) -> (Option<&[u8]>, &[T]) {
        let valid_map = self.validity.as_ref().map(|vm| vm.aligned().0);
        let data = self.data.cast();
        (valid_map, data)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
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

impl FromIterator<Option<i32>> for OwnFlat {
    #[inline]
    fn from_iter<T: IntoIterator<Item = Option<i32>>>(iter: T) -> Self {
        let mut bm = VecBitmap::new();
        let mut data = vec![];
        for v in iter {
            match v {
                Some(i) => {
                    bm.add(true).unwrap();
                    data.push(i);
                }
                None => {
                    bm.add(false).unwrap();
                    data.push(0);
                }
            }
        }
        let data = VecArray::from(data);
        OwnFlat {
            validity: Some(bm),
            data,
        }
    }
}
