use smallvec::{smallvec, SmallVec};
use std::io;

pub trait ByteRepr: Default + Copy {
    fn to_bytes(&self) -> SmallVec<[u8; 16]>;

    fn from_bytes(bs: &[u8]) -> Self;

    fn write_bytes(&self, buf: &mut [u8]);

    fn memcpy(src: &[Self], tgt: &mut [u8]);

    fn write_all<W: io::Write>(writer: &mut W, src: &[Self]) -> io::Result<usize>;
}

macro_rules! impl_num {
    ($ty:ty) => {
        impl ByteRepr for $ty {
            #[inline]
            fn to_bytes(&self) -> SmallVec<[u8; 16]> {
                self.to_ne_bytes().into_iter().collect()
            }

            #[inline]
            fn from_bytes(bs: &[u8]) -> Self {
                <$ty>::from_ne_bytes(bs.try_into().unwrap())
            }

            #[inline]
            fn write_bytes(&self, buf: &mut [u8]) {
                let bs = self.to_ne_bytes();
                buf.copy_from_slice(&bs);
            }

            #[inline]
            fn memcpy(src: &[Self], tgt: &mut [u8]) {
                use std::mem::size_of;
                assert_eq!(tgt.len(), src.len() * size_of::<$ty>());
                // SAFETY:
                //
                // pointer and length are guaranteed to be valid.
                unsafe {
                    let src = std::slice::from_raw_parts(
                        src.as_ptr() as *const u8,
                        src.len() * size_of::<$ty>(),
                    );
                    tgt.copy_from_slice(src);
                }
            }

            #[inline]
            fn write_all<W: io::Write>(writer: &mut W, src: &[Self]) -> io::Result<usize> {
                use std::mem::size_of;
                let n_bytes = src.len() * size_of::<$ty>();
                let src = unsafe { std::slice::from_raw_parts(src.as_ptr() as *const u8, n_bytes) };
                writer.write_all(src)?;
                Ok(n_bytes)
            }
        }
    };
}

impl_num!(i32);
impl_num!(i64);
impl_num!(u64);

impl ByteRepr for u8 {
    #[inline]
    fn to_bytes(&self) -> SmallVec<[u8; 16]> {
        smallvec![*self]
    }

    #[inline]
    fn from_bytes(bs: &[u8]) -> Self {
        bs[0]
    }

    #[inline]
    fn write_bytes(&self, buf: &mut [u8]) {
        buf[0] = *self;
    }

    #[inline]
    fn memcpy(src: &[u8], tgt: &mut [u8]) {
        assert_eq!(tgt.len(), src.len());
        tgt.copy_from_slice(src);
    }

    #[inline]
    fn write_all<W: io::Write>(writer: &mut W, src: &[u8]) -> io::Result<usize> {
        writer.write_all(src)?;
        Ok(src.len())
    }
}
