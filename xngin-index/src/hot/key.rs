use std::ops::Deref;
use std::mem;

const MAX_OWNED_KEY_LENGTH: usize = 15;

/// Key can be either a reference of byte slice,
/// or an owned byte array with specified length.
#[derive(Clone, Copy)]
pub enum Key<'a> {
    Ref(&'a [u8]),
    Owned([u8; MAX_OWNED_KEY_LENGTH], u8),
}

impl Deref for Key<'_> {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        match self {
            Key::Ref(r) => r,
            Key::Owned(data, len) => &data[..*len as usize],
        }
    }
}

/// Specify how to extract key.
pub trait ExtractKey {
    fn extract_key(&self) -> Key<'_>;
}

impl ExtractKey for str {
    #[inline]
    fn extract_key(&self) -> Key<'_> {
        Key::Ref(self.as_bytes())
    }
}

impl ExtractKey for [u8] {
    #[inline]
    fn extract_key(&self) -> Key<'_> {
        Key::Ref(self)
    }
}

macro_rules! impl_extract_key_for_int {
    ($ty:ty) => {
        impl ExtractKey for $ty {
            #[inline]
            fn extract_key(&self) -> Key<'_> {
                let mut data = [0u8; MAX_OWNED_KEY_LENGTH];
                let len = mem::size_of::<$ty>();
                data[..len].copy_from_slice(&self.to_be_bytes());
                Key::Owned(data, len as u8)
            }
        }
    }
}

impl_extract_key_for_int!(i64);
impl_extract_key_for_int!(u64);
impl_extract_key_for_int!(i32);
impl_extract_key_for_int!(u32);

/// Specify how to extract TID.
/// TID stands for tuple id, which is unique identifier
/// of a record.
/// Sometimes, if the tuple contains only small values 
/// that can be fit in 8 bytes, it can be directly 
/// embedded in TID.
pub trait ExtractTID {
    fn extract_tid(&self) -> u64;
}

macro_rules! impl_extract_tid_for_int {
    ($ty:ty) => {
        impl ExtractTID for $ty {
            #[inline]
            fn extract_tid(&self) -> u64 {
                *self as u64
            }
        }
    } 
}

impl_extract_tid_for_int!(i32);
impl_extract_tid_for_int!(u32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_size() {
        println!("size of Key is {}", std::mem::size_of::<Key>());
    }
}