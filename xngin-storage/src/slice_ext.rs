use std::marker::PhantomData;
use std::num::NonZeroUsize;

/// helper trait to iterator neighboring pairs over slice.
pub trait PairSliceExt<T> {
    fn pairs(&self) -> PairIter<'_, T>;
}

impl<T> PairSliceExt<T> for [T] {
    fn pairs(&self) -> PairIter<'_, T> {
        PairIter {
            ptr: self.as_ptr(),
            len: self.len(),
            _marker: PhantomData,
        }
    }
}

pub struct PairIter<'a, T: 'a> {
    ptr: *const T,
    len: usize,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: 'a> Iterator for PairIter<'a, T> {
    type Item = (&'a T, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.len < 2 {
            None
        } else {
            unsafe {
                let ptr1 = self.ptr.add(1);
                let v0 = &*self.ptr;
                let v1 = &*ptr1;
                self.ptr = self.ptr.add(1);
                self.len -= 1;
                Some((v0, v1))
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.len < 2 {
            (0, Some(0))
        } else {
            (0, Some(self.len - 1))
        }
    }
}

/// helper trait to iterate mutable pairs with offset over slice.
pub trait OffsetPairMut<T> {
    fn for_each_offset_pair<F>(&mut self, offset: usize, f: F)
    where
        F: FnMut((&mut T, &T));
}

impl<T> OffsetPairMut<T> for [T] {
    #[inline]
    fn for_each_offset_pair<F>(&mut self, offset: usize, f: F)
    where
        F: FnMut((&mut T, &T)),
    {
        let offset = NonZeroUsize::new(offset).expect("offset must be non-zero");
        let pair = PtrOffsetPairSlice::new(self, offset);
        pair.for_each(f);
    }
}

struct PtrOffsetPairSlice<'a, T: 'a> {
    ptr: *mut T,
    len: usize,
    offset: NonZeroUsize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T: 'a> PtrOffsetPairSlice<'a, T> {
    fn new(slice: &mut [T], offset: NonZeroUsize) -> Self {
        PtrOffsetPairSlice {
            ptr: slice.as_mut_ptr(),
            len: slice.len(),
            offset,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: 'a> Iterator for PtrOffsetPairSlice<'a, T> {
    type Item = (&'a mut T, &'a T);

    #[inline]
    fn next(&mut self) -> Option<(&'a mut T, &'a T)> {
        if self.len <= self.offset.get() {
            None
        } else {
            unsafe {
                let ptr1 = self.ptr.add(self.offset.get());
                let v0 = &mut *self.ptr;
                let v1 = &*ptr1;
                self.ptr = self.ptr.add(1);
                self.len -= 1;
                Some((v0, v1))
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.len <= self.offset.get() {
            (0, Some(0))
        } else {
            (0, Some(self.len - self.offset.get()))
        }
    }
}

/// helper trait to iterate mutable triples with offset over slice.
pub trait OffsetTripleMut<T> {
    fn for_each_offset_triple<F>(&mut self, offset1: usize, offset2: usize, f: F)
    where
        F: FnMut((&mut T, &T, &T));
}

impl<T> OffsetTripleMut<T> for [T] {
    fn for_each_offset_triple<F>(&mut self, offset1: usize, offset2: usize, f: F)
    where
        F: FnMut((&mut T, &T, &T)),
    {
        if offset1 >= offset2 {
            panic!("offset1 must be less than offset2");
        }
        let offset1 = NonZeroUsize::new(offset1).expect("offset must be non-zero");
        let offset2 = NonZeroUsize::new(offset2).expect("offset must be non-zero");
        let triple = PtrOffsetTripleSlice::new(self, offset1, offset2);
        triple.for_each(f);
    }
}

struct PtrOffsetTripleSlice<'a, T: 'a> {
    ptr: *mut T,
    len: usize,
    offset1: NonZeroUsize,
    offset2: NonZeroUsize,
    _marker: PhantomData<&'a mut T>,
}

impl<'a, T: 'a> PtrOffsetTripleSlice<'a, T> {
    #[inline]
    fn new(slice: &mut [T], offset1: NonZeroUsize, offset2: NonZeroUsize) -> Self {
        PtrOffsetTripleSlice {
            ptr: slice.as_mut_ptr(),
            len: slice.len(),
            offset1,
            offset2,
            _marker: PhantomData,
        }
    }
}

impl<'a, T: 'a> Iterator for PtrOffsetTripleSlice<'a, T> {
    type Item = (&'a mut T, &'a T, &'a T);

    #[inline]
    fn next(&mut self) -> Option<(&'a mut T, &'a T, &'a T)> {
        if self.len <= self.offset2.get() {
            None
        } else {
            unsafe {
                let ptr1 = self.ptr.add(self.offset1.get());
                let ptr2 = self.ptr.add(self.offset2.get());
                let v0 = &mut *self.ptr;
                let v1 = &*ptr1;
                let v2 = &*ptr2;
                self.ptr = self.ptr.add(1);
                self.len -= 1;
                Some((v0, v1, v2))
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.len <= self.offset2.get() {
            (0, Some(0))
        } else {
            (0, Some(self.len - self.offset2.get()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_pair1() {
        let mut vs = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        vs.for_each_offset_pair(1, |(a, b)| {
            *a += *b;
        });
        assert_eq!(vec![3, 5, 7, 9, 11, 13, 15, 17, 19, 10], vs);
    }

    #[test]
    fn test_offset_pair2() {
        let mut vs = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        vs.for_each_offset_pair(2, |(a, b)| {
            *a += *b;
        });
        assert_eq!(vec![4, 6, 8, 10, 12, 14, 16, 18, 9, 10], vs);
    }

    #[test]
    fn test_offset_triple1() {
        let mut vs = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        vs.for_each_offset_triple(1, 2, |(a, b, c)| {
            *a += *b + *c;
        });
        assert_eq!(vec![6, 9, 12, 15, 18, 21, 24, 27, 9, 10], vs);
    }

    #[test]
    fn test_offset_triple2() {
        let mut vs = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        vs.for_each_offset_triple(3, 4, |(a, b, c)| {
            *a += *b + *c;
        });
        assert_eq!(vec![10, 13, 16, 19, 22, 25, 7, 8, 9, 10], vs);
    }

    #[test]
    fn test_extend_pair() {
        let vs = vec![1, 2, 3, 4, 5];
        let mut res = vec![];
        res.extend(vs.pairs().map(|(&a, &b)| (a, b)));
        assert_eq!(vec![(1, 2), (2, 3), (3, 4), (4, 5)], res);
    }

    #[test]
    fn test_extend_triple() {
        let mut vs = vec![1, 2, 3, 4, 5];
        let mut res = vec![];
        res.extend(
            PtrOffsetTripleSlice::new(
                &mut vs,
                NonZeroUsize::new(1).unwrap(),
                NonZeroUsize::new(2).unwrap(),
            )
            .map(|(&mut a, &b, &c)| (a, b, c)),
        );
        assert_eq!(vec![(1, 2, 3), (2, 3, 4), (3, 4, 5)], res);
    }
}
