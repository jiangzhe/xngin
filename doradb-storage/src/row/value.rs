use std::mem::{self, MaybeUninit};

pub const VAR_LEN_INLINE: usize = 6;
pub const VAR_LEN_PREFIX: usize = 4;
const _: () = assert!(mem::size_of::<VarByteVal>() == 8);

#[derive(Debug, Clone, Copy)]
pub enum Val<'a> {
    Byte1(Byte1Val),
    Byte2(Byte2Val),
    Byte4(Byte4Val),
    Byte8(Byte8Val),
    VarByte(&'a [u8]),
}

impl<'a> From<u8> for Val<'a> {
    #[inline]
    fn from(value: u8) -> Self {
        Val::Byte1(value)
    }
}

impl<'a> From<i8> for Val<'a> {
    #[inline]
    fn from(value: i8) -> Self {
        Val::Byte1(value as u8)
    }
}

impl<'a> From<u16> for Val<'a> {
    #[inline]
    fn from(value: u16) -> Self {
        Val::Byte2(value)
    }
}

impl<'a> From<i16> for Val<'a> {
    #[inline]
    fn from(value: i16) -> Self {
        Val::Byte2(value as u16)
    }
}

impl<'a> From<u32> for Val<'a> {
    #[inline]
    fn from(value: u32) -> Self {
        Val::Byte4(value)
    }
}

impl<'a> From<i32> for Val<'a> {
    #[inline]
    fn from(value: i32) -> Self {
        Val::Byte4(value as u32)
    }
}

impl<'a> From<f32> for Val<'a> {
    #[inline]
    fn from(value: f32) -> Self {
        Val::Byte4(u32::from_ne_bytes(value.to_ne_bytes()))
    }
}

impl<'a> From<u64> for Val<'a> {
    #[inline]
    fn from(value: u64) -> Self {
        Val::Byte8(value)
    }
}

impl<'a> From<i64> for Val<'a> {
    #[inline]
    fn from(value: i64) -> Self {
        Val::Byte8(value as u64)
    }
}

impl<'a> From<f64> for Val<'a> {
    #[inline]
    fn from(value: f64) -> Self {
        Val::Byte8(u64::from_ne_bytes(value.to_ne_bytes()))
    }
}

impl<'a> From<&'a [u8]> for Val<'a> {
    #[inline]
    fn from(value: &'a [u8]) -> Self {
        Val::VarByte(value)
    }
}

impl<'a> From<&'a str> for Val<'a> {
    #[inline]
    fn from(value: &'a str) -> Self {
        Val::VarByte(value.as_bytes())
    }
}

/// Value is a marker trait to represent 
/// fixed-length column value in row page.
pub trait Value {}

pub trait ToValue {
    type Target: Value;

    fn to_val(&self) -> Self::Target;
}

pub type Byte1Val = u8;
pub trait Byte1ValSlice {
    fn as_i8s(&self) -> &[i8];

    fn as_i8s_mut(&mut self) -> &mut [i8];
}

impl Value for Byte1Val {}

impl Byte1ValSlice for [Byte1Val] {
    #[inline]
    fn as_i8s(&self) -> &[i8] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i8s_mut(&mut self) -> &mut [i8] {
        unsafe { mem::transmute(self) }
    }
}

impl ToValue for u8 {
    type Target = Byte1Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self
    }
}

impl ToValue for i8 {
    type Target = Byte1Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self as u8
    }
}

pub type Byte2Val = u16;
pub trait Byte2ValSlice {
    fn as_i16s(&self) -> &[i16];

    fn as_i16s_mut(&mut self) -> &mut [i16];
}
impl Value for Byte2Val {}

impl ToValue for u16 {
    type Target = Byte2Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self
    }
}

impl ToValue for i16 {
    type Target = Byte2Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self as u16
    }
}

impl Byte2ValSlice for [Byte2Val] {
    #[inline]
    fn as_i16s(&self) -> &[i16] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i16s_mut(&mut self) -> &mut [i16] {
        unsafe { mem::transmute(self) }
    }
}

pub type Byte4Val = u32;
pub trait Byte4ValSlice {
    fn as_i32s(&self) -> &[i32];

    fn as_i32s_mut(&mut self) -> &mut [i32];

    fn as_f32s(&self) -> &[f32];

    fn as_f32s_mut(&mut self) -> &mut [f32];
}

impl Value for Byte4Val {}

impl Byte4ValSlice for [Byte4Val] {
    #[inline]
    fn as_i32s(&self) -> &[i32] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i32s_mut(&mut self) -> &mut [i32] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_f32s(&self) -> &[f32] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_f32s_mut(&mut self) -> &mut [f32] {
        unsafe { mem::transmute(self) }
    }
}

impl ToValue for u32 {
    type Target = Byte4Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self
    }
}

impl ToValue for i32 {
    type Target = Byte4Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self as u32
    }
}

pub type Byte8Val = u64;
pub trait Byte8ValSlice {
    fn as_i64s(&self) -> &[i64];

    fn as_i64s_mut(&mut self) -> &mut [i64];

    fn as_f64s(&self) -> &[f64];

    fn as_f64s_mut(&mut self) -> &mut [f64];
}

impl Value for Byte8Val {}

impl ToValue for u64 {
    type Target = Byte8Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self
    }
}

impl ToValue for i64 {
    type Target = Byte8Val;
    #[inline]
    fn to_val(&self) -> Self::Target {
        *self as u64
    }
}

impl Byte8ValSlice for [Byte8Val] {
    #[inline]
    fn as_i64s(&self) -> &[i64] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_i64s_mut(&mut self) -> &mut [i64] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_f64s(&self) -> &[f64] {
        unsafe { mem::transmute(self) }
    }

    #[inline]
    fn as_f64s_mut(&mut self) -> &mut [f64] {
        unsafe { mem::transmute(self) }
    }
}

/// VarByteVal represents var-len value in page.
/// It has two kinds: inline and inpage.
/// Inline means the bytes are inlined in the fixed field.
/// Inpage means the fixed field only store length,
/// offset and prfix. Entire value is located at 
/// tail of page.
#[derive(Clone, Copy)]
pub struct VarByteVal {
    inner: VarByteInner,
}

impl VarByteVal {
    /// Create a new VarByteVal with inline data.
    /// The data length must be no more than 14 bytes.
    #[inline]
    pub fn inline(val: &[u8]) -> Self {
        debug_assert!(val.len() <= VAR_LEN_INLINE);
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
    pub fn inpage(len: u16, offset: u16, prefix: &[u8]) -> Self {
        debug_assert!(prefix.len() == VAR_LEN_PREFIX);
        let mut inpage = MaybeUninit::<Inpage>::uninit();
        unsafe {
            let p = inpage.assume_init_mut();
            p.len = len;
            p.offset = offset;
            p.prefix.copy_from_slice(prefix);
            VarByteVal{
                inner: VarByteInner{p: inpage.assume_init() }
            }
        }
    }

    /// Returns length of the value.
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { self.inner.v.len as usize }
    }

    /// Returns whether the value is inlined.
    #[inline]
    pub fn is_inlined(&self) -> bool {
        self.len() <= VAR_LEN_INLINE
    }

    /// Returns inpage length of given value.
    /// If the value can be inlined, returns 0.
    #[inline]
    pub fn inpage_len(data: &[u8]) -> usize {
        if data.len() > VAR_LEN_INLINE {
            data.len()
        } else {
            0
        }
    }

    /// Returns bytes.
    #[inline]
    pub fn as_bytes(&self, ptr: *const u8) -> &[u8] {
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

    /// Returns mutable bytes.
    #[inline]
    pub fn as_bytes_mut(&mut self, ptr: *mut u8) -> &mut [u8] {
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

    /// Returns string.
    #[inline]
    pub fn as_str(&self, ptr: *const u8) -> &str {
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

    /// Returns mutable string.
    #[inline]
    pub fn as_str_mut(&mut self, ptr: *mut u8) -> &mut str {
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

    /// In-place update with given value.
    /// Caller must ensure no extra space is required.
    #[inline]
    pub fn update_in_place(&mut self, ptr: *mut u8, val: &[u8]) {
        debug_assert!(val.len() <= VAR_LEN_INLINE || val.len() <= self.len());
        unsafe {
            if val.len() > VAR_LEN_INLINE {
                // all not inline, but original is longer or equal to input value.
                debug_assert!(self.len() > VAR_LEN_INLINE);
                self.inner.p.len = val.len() as u16;
                let target = std::slice::from_raw_parts_mut(ptr.add(self.inner.p.offset as usize), val.len());
                target.copy_from_slice(val);
            } else { // input is inlined.
                // better to reuse release page data.
                self.inner.v.len = val.len() as u16;
                self.inner.v.data[..val.len()].copy_from_slice(val);
            }
        }
    }
}

#[derive(Clone, Copy)]
union VarByteInner {
    v: Inline,
    p: Inpage,
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Inline {
    len: u16,
    data: [u8; VAR_LEN_INLINE],
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct Inpage {
    len: u16,
    offset: u16,
    prefix: [u8; VAR_LEN_PREFIX],
}