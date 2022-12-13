use super::node::NodeKind;
use std::arch::x86_64::{
    _mm256_cmpeq_epi16, _mm256_cmpeq_epi32, _mm256_cmpeq_epi8, _mm256_loadu_si256,
    _mm256_movemask_epi8, _mm256_set1_epi16, _mm256_set1_epi32, _mm256_set1_epi8, _pext_u32,
};

const PEXT_MASK_U16_FROM_U8: u32 = 0b1010_1010_1010_1010_1010_1010_1010_1010;
const PEXT_MASK_U32_FROM_U8: u32 = 0b1000_1000_1000_1000_1000_1000_1000_1000;

pub trait PartialKey: Sized + Copy + PartialEq {
    fn cast_from(src: u64) -> Self;

    /// Search partial key in given key array, and return matched mask.
    /// Note: the size of given key array must be greater or equal to 256 bits.
    ///       and only first 256 bits will be searched.
    unsafe fn mm256_search(self, keys_ptr: *const Self) -> u32;

    fn kind(single: bool) -> NodeKind;
}
impl PartialKey for u8 {
    #[inline]
    fn cast_from(src: u64) -> Self {
        src as Self
    }

    #[inline]
    unsafe fn mm256_search(self, keys_ptr: *const u8) -> u32 {
        let sparse_keys = _mm256_loadu_si256(keys_ptr as *const _);
        let key = _mm256_set1_epi8(self as i8);
        // let sel_bits = _mm256_and_si256(sparse_keys, key);
        let match_keys = _mm256_cmpeq_epi8(key, sparse_keys);
        let match_mask = _mm256_movemask_epi8(match_keys);
        match_mask as u32
    }

    #[inline]
    fn kind(single: bool) -> NodeKind {
        if single {
            NodeKind::SP8
        } else {
            NodeKind::MP8
        }
    }
}
impl PartialKey for u16 {
    #[inline]
    fn cast_from(src: u64) -> Self {
        src as Self
    }

    #[inline]
    unsafe fn mm256_search(self, keys_ptr: *const u16) -> u32 {
        let sparse_keys = _mm256_loadu_si256(keys_ptr as *const _);
        let key = _mm256_set1_epi16(self as i16);
        let match_keys = _mm256_cmpeq_epi16(key, sparse_keys);
        let match_mask = _mm256_movemask_epi8(match_keys);
        // extract u16 bits with mask
        _pext_u32(match_mask as u32, PEXT_MASK_U16_FROM_U8)
    }

    #[inline]
    fn kind(single: bool) -> NodeKind {
        if single {
            NodeKind::SP16
        } else {
            NodeKind::MP16
        }
    }
}
impl PartialKey for u32 {
    #[inline]
    fn cast_from(src: u64) -> Self {
        src as Self
    }

    #[inline]
    unsafe fn mm256_search(self, keys_ptr: *const u32) -> u32 {
        let sparse_keys = _mm256_loadu_si256(keys_ptr as *const _);
        let key = _mm256_set1_epi32(self as i32);
        let match_keys = _mm256_cmpeq_epi32(key, sparse_keys);
        let match_mask = _mm256_movemask_epi8(match_keys);
        // extract u32 bits with mask
        _pext_u32(match_mask as u32, PEXT_MASK_U32_FROM_U8)
    }

    #[inline]
    fn kind(single: bool) -> NodeKind {
        if single {
            NodeKind::SP32
        } else {
            NodeKind::MP32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::arch::x86_64::_pdep_u32;
    use std::arch::x86_64::_pext_u32;

    #[test]
    fn test_pext_pdep() {
        unsafe {
            for (src, mask, res) in [
                (0xffff, 0x00ff, 0xff),
                (0x010f, 0x0f0f, 0x1f),
                (0x020f, 0x010f, 0x0f),
            ] {
                assert_eq!(_pext_u32(src, mask), res);
            }

            for (src, mask, res) in [
                (0xffff, 0xff, 0xff),
                (0x010f, 0xff, 0x000f),
                (0x010f, 0xfffe, 0x021e),
            ] {
                assert_eq!(_pdep_u32(src, mask), res)
            }
        }
    }

    #[test]
    fn test_partial_key_u8_search() {
        let keys: Vec<_> = (0u8..32).collect();
        let keys_ptr = keys.as_ptr();
        for key in 0u8..32 {
            let mask = unsafe { key.mm256_search(keys_ptr) };
            let n = mask.count_ones();
            println!("key={}, mask={}, n={}", key, mask, n);
            assert_eq!(1, n);
        }
    }

    #[test]
    fn test_movemask_u16() {
        unsafe {
            let rs = vec![
                0xffffu16, 0, 0xffff, 0xffff, 0, 0, 0xffff, 0, 0, 0, 0, 0xffff, 0, 0, 0xffff,
                0xffff,
            ];
            let keys = _mm256_loadu_si256(rs.as_ptr() as *const _);
            let res_u8 = _mm256_movemask_epi8(keys) as u32;
            println!("res_u8={:032b}", res_u8);
            let res_u16 = _pext_u32(res_u8, PEXT_MASK_U16_FROM_U8);
            println!("res_u16={:032b}", res_u16);
        }
    }
}
