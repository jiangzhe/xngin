use crate::error::{Error, Result};
use xngin_common::bitmap::Bitmap;
use xngin_common::array::Array;
use xngin_storage::attr::Attr;
use xngin_storage::codec::{Codec, Single};
use xngin_datatype::PreciseType;
use smallvec::SmallVec;
use std::sync::Arc;

/// Sel encodes filter indexes into bitmap, single or none.
#[derive(Debug, Clone)]
pub enum Sel {
    Bitmap(Arc<Bitmap>),
    /// only one index encoded, the type is u16.
    Single {
        idx: u16,
        len: u16,
    },
    /// no value returned.
    None { len: u16 },
}

impl Sel {
    /// Returns total number of records.
    #[inline]
    pub fn n_records(&self) -> usize {
        match self {
            Sel::Bitmap(b) => b.len(),
            Sel::Single{len, ..} | Sel::None{len} => *len as usize,
        }
    }

    /// Returns number of filtered records.
    #[inline]
    pub fn n_filtered(&self) -> usize {
        match self {
            Sel::Bitmap(b) => b.true_count(),
            Sel::Single{..} => 1,
            Sel::None{..} => 0,
        }
    }

    /// Apply selection to given attribute.
    /// It generates a new attribute with only filtered values.
    #[inline]
    pub fn apply(&self, attr: &Attr) -> Result<Attr> {
        debug_assert_eq!(self.n_records(), attr.n_records());
        let res = match self {
            Sel::Bitmap(sel) => {
                let n_filtered = sel.true_count();
                match n_filtered {
                    0 => Attr::empty(attr.ty),
                    1 => {
                        let idx = sel.first_true().unwrap(); // won't fail
                        let (valid, raw_val) = attr.raw_val(idx)?;
                        if valid {
                            let mut data: SmallVec<_> = SmallVec::with_capacity(raw_val.len());
                            data.extend_from_slice(raw_val);
                            Attr::new_single(attr.ty, Single::raw_from_bytes(data, 1))
                        } else {
                            Attr::new_single(attr.ty, Single::new_null(1))
                        }
                    }
                    n if n == attr.n_records() => {
                        // retain all rows, just copy
                        attr.to_owned()
                    }
                    _ => {
                        // apply data
                        match &attr.codec {
                            Codec::Empty => unreachable!(),
                            Codec::Single(s) => if s.valid {
                                let mut s = s.clone();
                                s.len = n_filtered;
                                Attr::new_single(attr.ty, s)
                            } else {
                                Attr::new_single(attr.ty, Single::new_null(n_filtered))
                            }
                            Codec::Bitmap(b) => {
                                debug_assert_eq!(PreciseType::bool(), attr.ty);
                                // extend data
                                let mut data = Bitmap::with_capacity(n_filtered);
                                let mut idx = 0;
                                for (flag, n) in sel.range_iter() {
                                    if flag {
                                        data.extend_range(b, idx..idx+n)?;
                                    }
                                    idx += n;
                                }
                                debug_assert_eq!(data.len(), n_filtered);
                                // extend validity
                                let validity = if let Some(vm) = attr.validity.as_ref() {
                                    let mut tmp = Bitmap::with_capacity(n_filtered);
                                    idx = 0;
                                    for (flag, n) in sel.range_iter() {
                                        if flag {
                                            tmp.extend_range(vm, idx..idx+n)?;
                                        }
                                        idx += n;
                                    }
                                    debug_assert_eq!(tmp.len(), n_filtered);
                                    Some(Arc::new(tmp))
                                } else {
                                    None
                                };
                                Attr::new_bitmap(data, validity)
                            }
                            Codec::Array(a) => {
                                let mut data = Array::with_capacity(n_filtered);
                                let data_raw = data.raw_mut().unwrap(); // won't fail
                                let src_raw = a.raw();
                                let val_len = attr.ty.val_len().unwrap(); // won't fail
                                let mut src_idx = 0;
                                let mut tgt_offset = 0;
                                for (flag, n) in sel.range_iter() {
                                    if flag {
                                        let src_offset = src_idx*val_len;
                                        let raw_len = n * val_len;
                                        data_raw[tgt_offset..tgt_offset+raw_len].copy_from_slice(&src_raw[src_offset..src_offset+raw_len]);
                                        tgt_offset += raw_len;
                                    }
                                    src_idx += n;
                                }
                                debug_assert_eq!(n_filtered * val_len, tgt_offset);
                                // SAFETY
                                //
                                // length is guaranteed to be number of filtered elements.
                                unsafe { data.set_len(n_filtered) };
                                // apply validity
                                let validity = if let Some(vm) = attr.validity.as_ref() {
                                    let mut tmp = Bitmap::with_capacity(n_filtered);
                                    let mut idx = 0;
                                    for (flag, n) in sel.range_iter() {
                                        if flag {
                                            tmp.extend_range(vm, idx..idx+n)?;
                                        }
                                        idx += n;
                                    }
                                    debug_assert_eq!(tmp.len(), n_filtered);
                                    Some(Arc::new(tmp))
                                } else {
                                    None
                                };
                                Attr::new_array(attr.ty, data, validity, None)
                            }
                        }
                    }
                }
            }
            Sel::Single{idx, ..} => {
                let idx = *idx as usize;
                let (valid, raw_val) = attr.raw_val(idx)?;
                if valid {
                    let mut data: SmallVec<_> = SmallVec::with_capacity(raw_val.len());
                    data.extend_from_slice(raw_val);
                    Attr::new_single(attr.ty, Single::raw_from_bytes(data, 1))
                } else {
                    Attr::new_single(attr.ty, Single::new_null(1))
                }
            }
            Sel::None{..} => Attr::empty(attr.ty),
        };
        Ok(res)
    }
}

impl<'a> TryFrom<&'a Codec> for Sel {
    type Error = Error;
    #[inline]
    fn try_from(src: &Codec) -> Result<Self> {
        let res = match src {
            Codec::Single(s) => if s.valid {
                let (_, idx) = s.view::<u16>();
                if idx as usize >= s.len {
                    return Err(Error::IndexOutOfBound)
                }
                Sel::Single{idx, len: s.len as u16}
            } else {
                Sel::None{len: s.len as u16}
            },
            Codec::Bitmap(b) => Sel::Bitmap(Arc::clone(b)),
            _ => return Err(Error::InvalidCodecForSel),
        };
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sel_try_from() {
        let c = Codec::new_bitmap(Bitmap::from_iter(vec![true; 1024]));
        let sel = Sel::try_from(&c).unwrap();
        assert_eq!(1024, sel.n_records());
        assert_eq!(1024, sel.n_filtered());
        let c = Codec::Single(Single::new_null(1024));
        let sel = Sel::try_from(&c).unwrap();
        assert_eq!(1024, sel.n_records());
        assert_eq!(0, sel.n_filtered());
        let c = Codec::Single(Single::new(1u16, 1024));
        let sel = Sel::try_from(&c).unwrap();
        assert_eq!(1024, sel.n_records());
        assert_eq!(1, sel.n_filtered());
    }

    #[test]
    fn test_sel_apply_single() {
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(3, true).unwrap();
        bm.set(5, true).unwrap();
        bm.set(6, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let attr = Attr::new_single(PreciseType::i32(), Single::new(10i32, 1024));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(3, res.n_records());
        let attr = Attr::new_single(PreciseType::i32(), Single::new_null(1024));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(3, res.n_records());
        assert!(!res.codec.as_single().unwrap().valid);
    }
    
    #[test]
    fn test_sel_apply_array() {
        let attr = Attr::from(0i32..1024);
        // none
        let sel = Sel::None{len: 1024};
        let res = sel.apply(&attr).unwrap();
        assert_eq!(0, res.n_records());
        // single
        let sel = Sel::Single{idx: 1, len: 1024};
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1, res.n_records());
        assert_eq!((true, 1), res.codec.as_single().unwrap().view::<i32>());
        // single null
        let sel = Sel::Single{idx: 1, len: 1024};
        let mut bm = Bitmap::from_iter(vec![true; 1024]);
        bm.set(1, false).unwrap();
        let attr2 = Attr::new_array(PreciseType::i32(), Array::from(0i32..1024), Some(Arc::new(bm)), None);
        let res = sel.apply(&attr2).unwrap();
        assert_eq!(1, res.n_records());
        assert_eq!((false, 0), res.codec.as_single().unwrap().view::<i32>());
        // array 0
        let bm = Bitmap::from_iter(vec![false; 1024]);
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(0, res.n_records());
        // array 1
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(10, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1, res.n_records());
        // array 3
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(3, true).unwrap();
        bm.set(5, true).unwrap();
        bm.set(6, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(3, res.n_records());
        assert_eq!(&[3i32, 5, 6], res.codec.as_array().unwrap().cast_slice::<i32>());
        // array all
        let bm = Bitmap::from_iter(vec![true; 1024]);
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1024, res.n_records());
    }

    #[test]
    fn test_sel_apply_bitmap() {
        let attr = Attr::new_bitmap(Bitmap::from_iter(vec![true; 1024]), None);
        // none
        let sel = Sel::None{len: 1024};
        let res = sel.apply(&attr).unwrap();
        assert_eq!(0, res.n_records());
        // single
        let sel = Sel::Single{idx: 1, len: 1024};
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1, res.n_records());
        assert_eq!((true, 1), res.codec.as_single().unwrap().view::<u8>());
        // array 0
        let bm = Bitmap::from_iter(vec![false; 1024]);
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(0, res.n_records());
        // array 1
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(10, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(1, res.n_records());
        // array 3
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(3, true).unwrap();
        bm.set(5, true).unwrap();
        bm.set(6, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let res = sel.apply(&attr).unwrap();
        assert_eq!(3, res.n_records());
        assert_eq!(vec![true, true, true], res.codec.as_bitmap().unwrap().bools().collect::<Vec<bool>>());
        // array with validity
        let mut bm = Bitmap::from_iter(vec![false; 1024]);
        bm.set(3, true).unwrap();
        bm.set(5, true).unwrap();
        bm.set(6, true).unwrap();
        let sel = Sel::Bitmap(Arc::new(bm));
        let mut vm = Bitmap::from_iter(vec![true; 1024]);
        vm.set(3, false).unwrap();
        let attr2= Attr::new_bitmap(Bitmap::from_iter(vec![true; 1024]), Some(Arc::new(vm)));
        let res = sel.apply(&attr2).unwrap();
        assert_eq!(3, res.n_records());
        let (valid, _) = res.raw_val(0).unwrap();
        assert!(!valid);
    }
}