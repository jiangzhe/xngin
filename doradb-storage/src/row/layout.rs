/// ColLayout defines the memory layout of columns 
/// stored in row page. 
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColLayout {
    Byte1,   // i8, u8, bool, char(1) with ascii/latin charset
    Byte2,   // i16, u16, decimal(1-2)
    Byte4,   // i32, u32, f32, decimal(3-8)
    Byte8,   // i64, u64, f64, decimal(9-18)
    Byte16,  // decimal(19-38)
    VarByte, // bytes, string, no more than 60k.
}

impl ColLayout {
    #[inline]
    pub const fn fix_len(&self) -> usize {
        match self {
            ColLayout::Byte1 => 1,
            ColLayout::Byte2 => 2,
            ColLayout::Byte4 => 4,
            ColLayout::Byte8 => 8,
            ColLayout::Byte16 => 16,
            // 2-byte len, 2-byte offset, 12-byte prefix
            // or inline version, 2-byte len, at most 14 inline bytes
            ColLayout::VarByte => 16, 
        }
    }
}