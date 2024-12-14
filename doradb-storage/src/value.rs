use serde::de::Visitor;
use serde::{Deserialize, Serialize};
use std::alloc::{alloc, dealloc, Layout};
use std::fmt;
use std::mem::{self, ManuallyDrop, MaybeUninit};
// use bitcode::{Encode, Decode};

pub const PAGE_VAR_LEN_INLINE: usize = 6;
pub const PAGE_VAR_LEN_PREFIX: usize = 4;
const _: () = assert!(mem::size_of::<PageVar>() == 8);

pub const MEM_VAR_LEN_INLINE: usize = 14;
pub const MEM_VAR_LEN_PREFIX: usize = 6;
const _: () = assert!(mem::size_of::<MemVar>() == 16);

/// Val is value representation of row-store.
/// The variable-length data may require new allocation
/// because we cannot rely on page data.
#[derive(Clone, Serialize, Deserialize)]
pub enum Val {
    Byte1(Byte1Val),
    Byte2(Byte2Val),
    Byte4(Byte4Val),
    Byte8(Byte8Val),
    VarByte(MemVar),
}

unsafe impl Send for Val {}
unsafe impl Sync for Val {}

impl PartialEq for Val {
    #[inline]
    fn eq(&self, rhs: &Self) -> bool {
        match (self, rhs) {
            (Val::Byte1(l), Val::Byte1(r)) => l == r,
            (Val::Byte2(l), Val::Byte2(r)) => l == r,
            (Val::Byte4(l), Val::Byte4(r)) => l == r,
            (Val::Byte8(l), Val::Byte8(r)) => l == r,
            (Val::VarByte(l), Val::VarByte(r)) => l.as_bytes() == r.as_bytes(),
            _ => false,
        }
    }
}

impl Eq for Val {}

impl fmt::Debug for Val {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Val").finish()
    }
}

impl From<u8> for Val {
    #[inline]
    fn from(value: u8) -> Self {
        Val::Byte1(value)
    }
}

impl From<i8> for Val {
    #[inline]
    fn from(value: i8) -> Self {
        Val::Byte1(value as u8)
    }
}

impl From<u16> for Val {
    #[inline]
    fn from(value: u16) -> Self {
        Val::Byte2(value)
    }
}

impl From<i16> for Val {
    #[inline]
    fn from(value: i16) -> Self {
        Val::Byte2(value as u16)
    }
}

impl From<u32> for Val {
    #[inline]
    fn from(value: u32) -> Self {
        Val::Byte4(value)
    }
}

impl From<i32> for Val {
    #[inline]
    fn from(value: i32) -> Self {
        Val::Byte4(value as u32)
    }
}

impl From<u64> for Val {
    #[inline]
    fn from(value: u64) -> Self {
        Val::Byte8(value)
    }
}

impl From<i64> for Val {
    #[inline]
    fn from(value: i64) -> Self {
        Val::Byte8(value as u64)
    }
}

impl From<&[u8]> for Val {
    #[inline]
    fn from(value: &[u8]) -> Self {
        Val::VarByte(MemVar::new(value))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ValRef<'a> {
    Byte1(Byte1Val),
    Byte2(Byte2Val),
    Byte4(Byte4Val),
    Byte8(Byte8Val),
    VarByte(&'a [u8]),
}

impl<'a> From<u8> for ValRef<'a> {
    #[inline]
    fn from(value: u8) -> Self {
        ValRef::Byte1(value)
    }
}

impl<'a> From<i8> for ValRef<'a> {
    #[inline]
    fn from(value: i8) -> Self {
        ValRef::Byte1(value as u8)
    }
}

impl<'a> From<u16> for ValRef<'a> {
    #[inline]
    fn from(value: u16) -> Self {
        ValRef::Byte2(value)
    }
}

impl<'a> From<i16> for ValRef<'a> {
    #[inline]
    fn from(value: i16) -> Self {
        ValRef::Byte2(value as u16)
    }
}

impl<'a> From<u32> for ValRef<'a> {
    #[inline]
    fn from(value: u32) -> Self {
        ValRef::Byte4(value)
    }
}

impl<'a> From<i32> for ValRef<'a> {
    #[inline]
    fn from(value: i32) -> Self {
        ValRef::Byte4(value as u32)
    }
}

impl<'a> From<f32> for ValRef<'a> {
    #[inline]
    fn from(value: f32) -> Self {
        ValRef::Byte4(u32::from_ne_bytes(value.to_ne_bytes()))
    }
}

impl<'a> From<u64> for ValRef<'a> {
    #[inline]
    fn from(value: u64) -> Self {
        ValRef::Byte8(value)
    }
}

impl<'a> From<i64> for ValRef<'a> {
    #[inline]
    fn from(value: i64) -> Self {
        ValRef::Byte8(value as u64)
    }
}

impl<'a> From<f64> for ValRef<'a> {
    #[inline]
    fn from(value: f64) -> Self {
        ValRef::Byte8(u64::from_ne_bytes(value.to_ne_bytes()))
    }
}

impl<'a> From<&'a [u8]> for ValRef<'a> {
    #[inline]
    fn from(value: &'a [u8]) -> Self {
        ValRef::VarByte(value)
    }
}

impl<'a> From<&'a str> for ValRef<'a> {
    #[inline]
    fn from(value: &'a str) -> Self {
        ValRef::VarByte(value.as_bytes())
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

/// PageVar represents var-len value in page.
/// It has two kinds: inline and outline.
/// Inline means the bytes are inlined in the fixed field.
/// Outline means the fixed field only store length,
/// offset and prfix. Entire value is located at
/// tail of page.
#[derive(Clone, Copy)]
pub union PageVar {
    i: PageVarInline,
    o: PageVarOutline,
}

impl PageVar {
    /// Create a new PageVar with inline data.
    /// The data length must be no more than 6 bytes.
    #[inline]
    pub fn inline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= PAGE_VAR_LEN_INLINE);
        let mut inline = MaybeUninit::<PageVarInline>::uninit();
        unsafe {
            let i = inline.assume_init_mut();
            i.len = data.len() as u16;
            i.data[..data.len()].copy_from_slice(data);
            PageVar {
                i: inline.assume_init(),
            }
        }
    }

    /// Create a new PageVar with pointer info.
    /// The prefix length must be 4 bytes.
    #[inline]
    pub fn outline(len: u16, offset: u16, prefix: &[u8]) -> Self {
        debug_assert!(prefix.len() == PAGE_VAR_LEN_PREFIX);
        let mut outline = MaybeUninit::<PageVarOutline>::uninit();
        unsafe {
            let p = outline.assume_init_mut();
            p.len = len;
            p.offset = offset;
            p.prefix.copy_from_slice(prefix);
            PageVar {
                o: outline.assume_init(),
            }
        }
    }

    /// Returns length of the value.
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { self.i.len as usize }
    }

    /// Returns whether the value is inlined.
    #[inline]
    pub fn is_inlined(&self) -> bool {
        self.len() <= PAGE_VAR_LEN_INLINE
    }

    /// Returns inpage length of given value.
    /// If the value can be inlined, returns 0.
    #[inline]
    pub fn outline_len(data: &[u8]) -> usize {
        if data.len() > PAGE_VAR_LEN_INLINE {
            data.len()
        } else {
            0
        }
    }

    /// Returns bytes.
    #[inline]
    pub fn as_bytes(&self, ptr: *const u8) -> &[u8] {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            unsafe { &self.i.data[..len] }
        } else {
            unsafe {
                let data = ptr.add(self.o.offset as usize);
                std::slice::from_raw_parts(data, len)
            }
        }
    }

    /// Returns mutable bytes.
    #[inline]
    pub fn as_bytes_mut(&mut self, ptr: *mut u8) -> &mut [u8] {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            unsafe { &mut self.i.data[..len] }
        } else {
            unsafe {
                let data = ptr.add(self.o.offset as usize);
                std::slice::from_raw_parts_mut(data, len)
            }
        }
    }

    /// Returns string.
    #[inline]
    pub fn as_str(&self, ptr: *const u8) -> &str {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            unsafe { std::str::from_utf8_unchecked(&self.i.data[..len]) }
        } else {
            unsafe {
                let data = ptr.add(self.o.offset as usize);
                let bytes = std::slice::from_raw_parts(data, len);
                std::str::from_utf8_unchecked(bytes)
            }
        }
    }

    /// Returns mutable string.
    #[inline]
    pub fn as_str_mut(&mut self, ptr: *mut u8) -> &mut str {
        let len = self.len();
        if len <= PAGE_VAR_LEN_INLINE {
            unsafe { std::str::from_utf8_unchecked_mut(&mut self.i.data[..len]) }
        } else {
            unsafe {
                let data = ptr.add(self.o.offset as usize);
                let bytes = std::slice::from_raw_parts_mut(data, len);
                std::str::from_utf8_unchecked_mut(bytes)
            }
        }
    }

    /// In-place update with given value.
    /// Caller must ensure no extra space is required.
    #[inline]
    pub fn update_in_place(&mut self, ptr: *mut u8, val: &[u8]) {
        debug_assert!(val.len() <= PAGE_VAR_LEN_INLINE || val.len() <= self.len());
        unsafe {
            if val.len() > PAGE_VAR_LEN_INLINE {
                // all not inline, but original is longer or equal to input value.
                debug_assert!(self.len() > PAGE_VAR_LEN_INLINE);
                self.o.len = val.len() as u16;
                let target =
                    std::slice::from_raw_parts_mut(ptr.add(self.o.offset as usize), val.len());
                target.copy_from_slice(val);
            } else {
                // input is inlined.
                // better to reuse release page data.
                self.i.len = val.len() as u16;
                self.i.data[..val.len()].copy_from_slice(val);
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct PageVarInline {
    len: u16,
    data: [u8; PAGE_VAR_LEN_INLINE],
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct PageVarOutline {
    len: u16,
    offset: u16,
    prefix: [u8; PAGE_VAR_LEN_PREFIX],
}

/// VarBytes is similar to PageVar, but more general to use.
/// It does not depend on page data.
pub union MemVar {
    i: MemVarInline,
    o: ManuallyDrop<MemVarOutline>,
}

impl MemVar {
    /// Create a new MemVar.
    #[inline]
    pub fn new(data: &[u8]) -> Self {
        debug_assert!(data.len() <= 0xffff);
        if data.len() <= MEM_VAR_LEN_INLINE {
            Self::inline(data)
        } else {
            Self::outline(data)
        }
    }

    /// Create a new MemVar with inline data.
    /// The data length must be no more than 14 bytes.
    #[inline]
    pub fn inline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= MEM_VAR_LEN_INLINE);
        let mut inline = MaybeUninit::<MemVarInline>::uninit();
        unsafe {
            let i = inline.assume_init_mut();
            i.len = data.len() as u16;
            i.data[..data.len()].copy_from_slice(data);
            MemVar {
                i: inline.assume_init(),
            }
        }
    }

    /// Create a new outlined PageVar.
    #[inline]
    pub fn outline(data: &[u8]) -> Self {
        debug_assert!(data.len() <= 0xffff); // must be in range of u16
        let mut outline = MaybeUninit::<MemVarOutline>::uninit();
        unsafe {
            let o = outline.assume_init_mut();
            o.len = data.len() as u16;
            o.prefix.copy_from_slice(&data[..MEM_VAR_LEN_PREFIX]);
            let layout = Layout::from_size_align_unchecked(data.len(), 1);
            o.ptr = alloc(layout);
            let bs = std::slice::from_raw_parts_mut(o.ptr, data.len());
            bs.copy_from_slice(data);
            MemVar {
                o: ManuallyDrop::new(outline.assume_init()),
            }
        }
    }

    /// Returns length of the value.
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { self.i.len as usize }
    }

    /// Returns whether the value is inlined.
    #[inline]
    pub fn is_inlined(&self) -> bool {
        self.len() <= MEM_VAR_LEN_INLINE
    }

    /// Returns inpage length of given value.
    /// If the value can be inlined, returns 0.
    #[inline]
    pub fn outline_len(data: &[u8]) -> usize {
        if data.len() > MEM_VAR_LEN_INLINE {
            data.len()
        } else {
            0
        }
    }

    /// Returns bytes.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        let len = self.len();
        if len <= MEM_VAR_LEN_INLINE {
            unsafe { &self.i.data[..len] }
        } else {
            unsafe { std::slice::from_raw_parts(self.o.ptr, len) }
        }
    }

    /// Returns string.
    #[inline]
    pub fn as_str(&self) -> &str {
        let len = self.len();
        if len <= MEM_VAR_LEN_INLINE {
            unsafe { std::str::from_utf8_unchecked(&self.i.data[..len]) }
        } else {
            unsafe {
                let bytes = std::slice::from_raw_parts(self.o.ptr, len);
                std::str::from_utf8_unchecked(bytes)
            }
        }
    }
}

impl Clone for MemVar {
    #[inline]
    fn clone(&self) -> Self {
        unsafe {
            if self.len() > MEM_VAR_LEN_INLINE {
                MemVar { o: self.o.clone() }
            } else {
                MemVar { i: self.i }
            }
        }
    }
}

impl Drop for MemVar {
    #[inline]
    fn drop(&mut self) {
        let len = self.len();
        if len > MEM_VAR_LEN_INLINE {
            unsafe {
                let layout = Layout::from_size_align_unchecked(len, 1);
                dealloc(self.o.ptr, layout);
            }
        }
    }
}

impl Serialize for MemVar {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(self.as_bytes())
    }
}

impl<'de> Deserialize<'de> for MemVar {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_bytes(MemVarVisitor)
    }
}

struct MemVarVisitor;

impl<'de> Visitor<'de> for MemVarVisitor {
    type Value = MemVar;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("byte array")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() >= 0xffff {
            return fail_long_bytes();
        }
        Ok(MemVar::new(v))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() >= 0xffff {
            return fail_long_bytes();
        }
        Ok(MemVar::new(&v))
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() >= 0xffff {
            return fail_long_bytes();
        }
        Ok(MemVar::new(v.as_bytes()))
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.len() >= 0xffff {
            return fail_long_bytes();
        }
        Ok(MemVar::new(v.as_bytes()))
    }
}

#[inline]
fn fail_long_bytes<T, E: serde::de::Error>() -> Result<T, E> {
    Err(serde::de::Error::custom(
        "MemVar does not support bytes longer than u16:MAX",
    ))
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct MemVarInline {
    len: u16,
    data: [u8; MEM_VAR_LEN_INLINE],
}

#[derive(PartialEq, Eq)]
#[repr(C)]
struct MemVarOutline {
    len: u16,
    prefix: [u8; MEM_VAR_LEN_PREFIX],
    ptr: *mut u8,
}

impl Clone for MemVarOutline {
    #[inline]
    fn clone(&self) -> Self {
        unsafe {
            let layout = Layout::from_size_align_unchecked(self.len as usize, 1);
            let ptr = alloc(layout);
            MemVarOutline {
                len: self.len,
                prefix: self.prefix,
                ptr,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_var() {
        let var1 = PageVar::inline(b"hello");
        assert!(var1.is_inlined());
        assert!(var1.len() == 5);
        assert!(var1.as_bytes(std::ptr::null()) == b"hello");
    }

    #[test]
    fn test_mem_var() {
        let var1 = MemVar::new(b"hello");
        assert!(var1.is_inlined());
        assert!(var1.len() == 5);
        assert!(var1.as_bytes() == b"hello");
        assert!(var1.as_str() == "hello");
        assert!(MemVar::outline_len(b"hello") == 0);

        let var2 = MemVar::new(b"a long value stored outline");
        assert!(!var2.is_inlined());
        assert!(var2.len() == 27);
        assert!(var2.as_bytes() == b"a long value stored outline");
        assert!(var2.as_str() == "a long value stored outline");
        assert!(MemVar::outline_len(b"a long value stored outline") == 27);
    }
}
