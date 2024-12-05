use std::mem::{self, MaybeUninit};

pub trait SliceGuard {}

pub trait Value {

    fn as_u8(&self) -> u8 {
        unimplemented!()
    }

    fn as_u8_mut(&mut self) -> &mut u8 {
        unimplemented!()
    }

    fn as_i8(&self) -> i8 {
        unimplemented!()
    }

    fn as_i8_mut(&mut self) -> &mut i8 {
        unimplemented!()
    }

    fn as_u16(&self) -> u16 {
        unimplemented!()
    }

    fn as_u16_mut(&mut self) -> &mut u16 {
        unimplemented!()
    }

    fn as_i16(&self) -> i16 {
        unimplemented!()
    }

    fn as_i16_mut(&mut self) -> &mut i16 {
        unimplemented!()
    }

    fn as_u32(&self) -> u32 {
        unimplemented!()
    }

    fn as_u32_mut(&mut self) -> &mut u32 {
        unimplemented!()
    }

    fn as_i32(&self) -> i32 {
        unimplemented!()
    }

    fn as_i32_mut(&mut self) -> &mut i32 {
        unimplemented!()
    }

    fn as_u64(&self) -> u64 {
        unimplemented!()
    }

    fn as_u64_mut(&mut self) -> &mut u64 {
        unimplemented!()
    }

    fn as_i64(&self) -> i64 {
        unimplemented!()
    }

    fn as_i64_mut(&mut self) -> &mut i64 {
        unimplemented!()
    }

    // fn as_u128(&self) -> u128 {
    //     unimplemented!()
    // }

    // fn as_u128_mut(&mut self) -> &mut u128 {
    //     unimplemented!()
    // }

    fn as_bytes(&self, ptr: *const u8) -> &[u8] {
        unimplemented!()
    }

    fn as_bytes_mut(&mut self, ptr: *mut u8) -> &mut [u8] {
        unimplemented!()
    }

    fn as_str(&self, ptr: *const u8) -> &str {
        unimplemented!()
    }

    fn as_str_mut(&mut self, ptr: *mut u8) -> &mut str {
        unimplemented!()
    }
}

pub trait ValueSlice {
    fn as_u8(&self) -> &[u8] {
        unimplemented!()
    }

    fn as_u8_mut(&mut self) -> &mut [u8] {
        unimplemented!()
    }

    fn as_i8(&self) -> &[i8] {
        unimplemented!()
    }

    fn as_i8_mut(&mut self) -> &mut [i8] {
        unimplemented!()
    }

    fn as_u16(&self) -> &[u16] {
        unimplemented!()
    }

    fn as_u16_mut(&mut self) -> &mut [u16] {
        unimplemented!()
    }

    fn as_i16(&self) -> &[i16] {
        unimplemented!()
    }

    fn as_i16_mut(&mut self) -> &mut [i16] {
        unimplemented!()
    }

    fn as_u32(&self) -> &[u32] {
        unimplemented!()
    }

    fn as_u32_mut(&mut self) -> &mut [u32] {
        unimplemented!()
    }

    fn as_i32(&self) -> &[i32] {
        unimplemented!()
    }

    fn as_i32_mut(&mut self) -> &mut [i32] {
        unimplemented!()
    }

    fn as_u64(&self) -> &[u64] {
        unimplemented!()
    }

    fn as_u64_mut(&mut self) -> &mut [u64] {
        unimplemented!()
    }

    fn as_i64(&self) -> &[i64] {
        unimplemented!()
    }

    fn as_i64_mut(&mut self) -> &mut [i64] {
        unimplemented!()
    }

    fn as_u128(&self) -> &[u128] {
        unimplemented!()
    }

    fn as_u128_mut(&mut self) -> &mut [u128] {
        unimplemented!()
    }
}

pub type Byte1Val = u8;

impl Value for Byte1Val {
    #[inline]
    fn as_u8(&self) -> u8 {
        *self
    }

    #[inline]
    fn as_u8_mut(&mut self) -> &mut u8 {
        self
    }

    #[inline]
    fn as_i8(&self) -> i8 {
        unsafe { mem::transmute(*self) }
    }

    #[inline]
    fn as_i8_mut(&mut self) -> &mut i8 {
        unsafe { mem::transmute(self) }
    }
}

impl ValueSlice for [Byte1Val] {
    #[inline]
    fn as_u8(&self) -> &[u8] {
        self
    }

    #[inline]
    fn as_u8_mut(&mut self) -> &mut [u8] {
        self
    }

    #[inline]
    fn as_i8(&self) -> &[i8] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i8_mut(&mut self) -> &mut [i8] {
        unsafe { mem::transmute(self) }
    }
}

pub type Byte2Val = u16;

impl Value for Byte2Val {
    #[inline]
    fn as_u16(&self) -> u16 {
        *self
    }

    #[inline]
    fn as_u16_mut(&mut self) -> &mut u16 {
        self
    }

    #[inline]
    fn as_i16(&self) -> i16 {
        unsafe { mem::transmute(*self) }
    }

    #[inline]
    fn as_i16_mut(&mut self) -> &mut i16 {
        unsafe { mem::transmute(self) }
    }
}

impl ValueSlice for [Byte2Val] {
    #[inline]
    fn as_u16(&self) -> &[u16] {
        self
    }

    #[inline]
    fn as_u16_mut(&mut self) -> &mut [u16] {
        self
    }

    #[inline]
    fn as_i16(&self) -> &[i16] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i16_mut(&mut self) -> &mut [i16] {
        unsafe { mem::transmute(self) }
    }
}

pub type Byte4Val = u32;

impl Value for Byte4Val {
    #[inline]
    fn as_u32(&self) -> u32 {
        *self
    }

    #[inline]
    fn as_u32_mut(&mut self) -> &mut u32 {
        self
    }

    #[inline]
    fn as_i32(&self) -> i32 {
        unsafe { mem::transmute(*self) }
    }

    #[inline]
    fn as_i32_mut(&mut self) -> &mut i32 {
        unsafe { mem::transmute(self) }
    }
}

impl ValueSlice for [Byte4Val] {
    #[inline]
    fn as_u32(&self) -> &[u32] {
        self
    }

    #[inline]
    fn as_u32_mut(&mut self) -> &mut [u32] {
        self
    }

    #[inline]
    fn as_i32(&self) -> &[i32] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i32_mut(&mut self) -> &mut [i32] {
        unsafe { mem::transmute(self) }
    }
}

pub type Byte8Val = u64;

impl Value for Byte8Val {
    #[inline]
    fn as_u64(&self) -> u64 {
        *self
    }

    #[inline]
    fn as_u64_mut(&mut self) -> &mut u64 {
        self
    }

    #[inline]
    fn as_i64(&self) -> i64 {
        unsafe { mem::transmute(*self) }
    }

    #[inline]
    fn as_i64_mut(&mut self) -> &mut i64 {
        unsafe { mem::transmute(self) }
    }
}

impl ValueSlice for [Byte8Val] {
    #[inline]
    fn as_u64(&self) -> &[u64] {
        self
    }

    #[inline]
    fn as_u64_mut(&mut self) -> &mut [u64] {
        self
    }

    #[inline]
    fn as_i64(&self) -> &[i64] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i64_mut(&mut self) -> &mut [i64] {
        unsafe { mem::transmute(self) }
    }
}

pub struct VarByteVal {
    inner: VarByteInner,
}

impl VarByteVal {
    /// Create a new VarByteVal with inline data.
    /// The data length must be no more than 14 bytes.
    #[inline]
    pub fn inline(val: &[u8]) -> Self {
        debug_assert!(val.len() <= 14);
        let mut inline = MaybeUninit::<Inline>::uninit();
        unsafe {
            let  i = inline.assume_init_mut();
            i.len = val.len() as u16;
            i.data[..val.len()].copy_from_slice(val);
            VarByteVal{
                inner: VarByteInner{v: inline.assume_init()},
            }
        }
    }

    /// Create a new VarByteVal with pointer info.
    /// The prefix length must be 12 bytes.
    #[inline]
    pub fn pointer(len: u16, offset: u16, prefix: &[u8]) -> Self {
        debug_assert!(prefix.len() == 12);
        let mut pointer = MaybeUninit::<Pointer>::uninit();
        unsafe {
            let p = pointer.assume_init_mut();
            p.len = len;
            p.offset = offset;
            p.prefix.copy_from_slice(prefix);
            VarByteVal{
                inner: VarByteInner{p: pointer.assume_init() }
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        unsafe { self.inner.v.len as usize }
    }

}

impl Value for VarByteVal {
    #[inline]
    fn as_bytes(&self, ptr: *const u8) -> &[u8] {
        let len = self.len();
        if len <= 14 {
            unsafe { &self.inner.v.data[..len] }
        } else {
            unsafe { 
                let data = ptr.add(self.inner.p.offset as usize);
                std::slice::from_raw_parts(data, len)
            }
        }
    }

    #[inline]
    fn as_bytes_mut(&mut self, ptr: *mut u8) -> &mut [u8] {
        let len = self.len();
        if len <= 14 {
            unsafe { &mut self.inner.v.data[..len] }
        } else {
            unsafe { 
                let data = ptr.add(self.inner.p.offset as usize);
                std::slice::from_raw_parts_mut(data, len)
            }
        }
    }

    #[inline]
    fn as_str(&self, ptr: *const u8) -> &str {
        let len = self.len();
        if len <= 14 {
            unsafe { std::str::from_utf8_unchecked(&self.inner.v.data[..len]) }
        } else {
            unsafe { 
                let data = ptr.add(self.inner.p.offset as usize);
                let bytes = std::slice::from_raw_parts(data, len);
                std::str::from_utf8_unchecked(bytes)
            }
        }
    }

    #[inline]
    fn as_str_mut(&mut self, ptr: *mut u8) -> &mut str {
        let len = self.len();
        if len <= 14 {
            unsafe { std::str::from_utf8_unchecked_mut(&mut self.inner.v.data[..len]) }
        } else {
            unsafe { 
                let data = ptr.add(self.inner.p.offset as usize);
                let bytes = std::slice::from_raw_parts_mut(data, len);
                std::str::from_utf8_unchecked_mut(bytes)
            }
        }
    }
}

impl ValueSlice for [VarByteVal] {}

union VarByteInner {
    v: Inline,
    p: Pointer,
}

#[derive(Debug, Clone, Copy)]
struct Inline {
    len: u16,
    data: [u8; 14],
}

#[derive(Debug, Clone, Copy)]
struct Pointer {
    len: u16,
    offset: u16,
    prefix: [u8; 12],
}