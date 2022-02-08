use crate::DataType;
use static_init::dynamic;

#[derive(Debug, Clone, Copy)]
pub struct PreciseType {
    pub ty: DataType,
    pub detail: Detail,
}

impl PreciseType {
    #[inline]
    pub fn decimal(max_intg: u8, max_frac: u8) -> Self {
        PreciseType {
            ty: DataType::Decimal,
            detail: Detail::Decimal { max_intg, max_frac },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Detail {
    None,
    Decimal { max_intg: u8, max_frac: u8 },
}

#[dynamic]
pub static I64: PreciseType = PreciseType {
    ty: DataType::I64,
    detail: Detail::None,
};

#[dynamic]
pub static U64: PreciseType = PreciseType {
    ty: DataType::U64,
    detail: Detail::None,
};

#[dynamic]
pub static F64: PreciseType = PreciseType {
    ty: DataType::F64,
    detail: Detail::None,
};
