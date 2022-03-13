use smallvec::SmallVec;

pub trait ByteRepr: Default {
    fn to_bytes(&self) -> SmallVec<[u8; 16]>;

    fn from_bytes(bs: &[u8]) -> Self;
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
        }
    };
}

impl_num!(i32);
impl_num!(i64);
