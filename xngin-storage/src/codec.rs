use smallvec::{smallvec, SmallVec};
use std::sync::Arc;
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
#[derive(Debug)]
pub enum Codec {
    Single(SingleCodec),
    Flat(FlatCodec),
}

#[allow(clippy::len_without_is_empty)]
impl Codec {
    #[inline]
    pub fn new_flat(validity: Option<VecBitmap>, data: VecArray) -> Self {
        Codec::Flat(FlatCodec::Owned(Arc::new(OwnFlat { validity, data })))
    }

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

    #[inline]
    pub fn to_owned(&self) -> Self {
        match self {
            Codec::Single(s) => Codec::Single(s.clone()),
            Codec::Flat(f) => Codec::Flat(f.to_owned()),
        }
    }

    #[inline]
    pub fn into_owned(self) -> Self {
        match self {
            c @ Codec::Single(_) | c @ Codec::Flat(FlatCodec::Owned(_)) => c,
            Codec::Flat(FlatCodec::Borrowed(bf)) => {
                Codec::Flat(FlatCodec::Owned(Arc::new(OwnFlat::from_view(&bf))))
            }
        }
    }

    #[inline]
    pub fn as_flat(&self) -> Option<&FlatCodec> {
        match self {
            Codec::Flat(f) => Some(f),
            _ => None,
        }
    }
}

impl<T, I> From<I> for Codec
where
    T: ByteRepr,
    I: ExactSizeIterator<Item = T>,
{
    #[inline]
    fn from(src: I) -> Self {
        Codec::Flat(FlatCodec::Owned(Arc::new(OwnFlat::from(src))))
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
#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub enum FlatCodec {
    Borrowed(BorrowFlat),
    Owned(Arc<OwnFlat>),
}

#[allow(clippy::len_without_is_empty)]
impl FlatCodec {
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

    #[inline]
    pub fn to_owned(&self) -> Self {
        match self {
            FlatCodec::Borrowed(bf) => FlatCodec::Owned(Arc::new(OwnFlat::from_view(bf))),
            FlatCodec::Owned(of) => FlatCodec::Owned(Arc::clone(of)),
        }
    }
}

impl<T, I> From<I> for FlatCodec
where
    T: ByteRepr,
    I: ExactSizeIterator<Item = T>,
{
    #[inline]
    fn from(src: I) -> Self {
        let owned = OwnFlat::from(src);
        FlatCodec::Owned(Arc::new(owned))
    }
}

impl<T: ByteRepr + Default> FromIterator<Option<T>> for FlatCodec {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let owned = OwnFlat::from_iter(iter);
        FlatCodec::Owned(Arc::new(owned))
    }
}

/// Flat codec represents uncompressed data of one attribute.
#[derive(Debug)]
pub struct BorrowFlat {
    validity: Option<ViewBitmap>,
    data: ViewArray,
}

#[allow(clippy::len_without_is_empty)]
impl BorrowFlat {
    #[inline]
    pub fn view<T: ByteRepr>(&self) -> (Option<&[u8]>, &[T]) {
        let valid_map = self.validity.as_ref().map(|vm| vm.aligned_u64().0);
        let data = self.data.cast();
        (valid_map, data)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

#[derive(Debug, Clone)]
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
        let valid_map = self.validity.as_ref().map(|vm| vm.aligned_u64().0);
        let data = self.data.cast();
        (valid_map, data)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn from_view(view: &BorrowFlat) -> Self {
        let validity = view.validity.as_ref().map(VecBitmap::from_view);
        let data = VecArray::from_view(&view.data);
        OwnFlat { validity, data }
    }
}

impl<T, I> From<I> for OwnFlat
where
    T: ByteRepr,
    I: ExactSizeIterator<Item = T>,
{
    #[inline]
    fn from(src: I) -> Self {
        OwnFlat {
            validity: None,
            data: VecArray::from(src),
        }
    }
}

impl<T: ByteRepr + Default> FromIterator<Option<T>> for OwnFlat {
    #[inline]
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let mut bm = VecBitmap::new();
        let mut data = vec![];
        let mut all_valid = true;
        for v in iter {
            match v {
                Some(i) => {
                    bm.add(true).unwrap();
                    data.push(i);
                }
                None => {
                    all_valid = false;
                    bm.add(false).unwrap();
                    data.push(T::default());
                }
            }
        }
        assert!(data.len() < u16::MAX as usize);
        let data = VecArray::from(data.into_iter());
        OwnFlat {
            validity: if all_valid { None } else { Some(bm) },
            data,
        }
    }
}
