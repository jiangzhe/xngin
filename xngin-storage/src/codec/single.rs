use smallvec::{smallvec, SmallVec};
use xngin_common::byte_repr::ByteRepr;

/// Single codec encodes single value as bytes in case all
/// values of that attribute are identical.
#[derive(Debug, Clone)]
pub struct Single {
    pub valid: bool,
    pub data: SmallVec<[u8; 16]>,
    pub len: usize,
}

#[allow(clippy::len_without_is_empty)]
impl Single {
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
        Single {
            valid: false,
            data: smallvec![],
            len,
        }
    }

    #[inline]
    pub fn new<T: ByteRepr>(val: T, len: usize) -> Self {
        Single {
            valid: true,
            data: val.to_bytes(),
            len,
        }
    }

    #[inline]
    pub fn raw_from_bytes(data: SmallVec<[u8; 16]>, len: usize) -> Self {
        Single {
            valid: true,
            data,
            len,
        }
    }
}
