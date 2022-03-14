use crate::runtime::RuntimeType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PreciseType {
    pub rty: RuntimeType,
    pub detail: Detail,
}

impl PreciseType {
    #[inline]
    pub fn null() -> Self {
        PreciseType {
            rty: RuntimeType::Null,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn bool() -> Self {
        PreciseType {
            rty: RuntimeType::Bool,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn i32() -> Self {
        PreciseType {
            rty: RuntimeType::I32,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn i64() -> Self {
        PreciseType {
            rty: RuntimeType::I64,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn u64() -> Self {
        PreciseType {
            rty: RuntimeType::U64,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn f64() -> Self {
        PreciseType {
            rty: RuntimeType::F64,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn decimal(max_intg: u8, max_frac: u8) -> Self {
        PreciseType {
            rty: RuntimeType::Decimal,
            detail: Detail::Decimal { max_intg, max_frac },
        }
    }

    #[inline]
    pub fn decimal_unknown() -> Self {
        PreciseType {
            rty: RuntimeType::Decimal,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn char(len: u16) -> Self {
        PreciseType {
            rty: RuntimeType::String,
            detail: Detail::Char { len },
        }
    }

    #[inline]
    pub fn varchar(max_len: u16) -> Self {
        PreciseType {
            rty: RuntimeType::String,
            detail: Detail::Varchar { max_len },
        }
    }

    #[inline]
    pub fn varchar_unknown() -> Self {
        PreciseType {
            rty: RuntimeType::String,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn bytes_unknown() -> Self {
        PreciseType {
            rty: RuntimeType::Bytes,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn date() -> Self {
        PreciseType {
            rty: RuntimeType::Date,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn time(prec: u8) -> Self {
        PreciseType {
            rty: RuntimeType::Time,
            detail: Detail::Time { prec },
        }
    }

    #[inline]
    pub fn time_unknown() -> Self {
        PreciseType {
            rty: RuntimeType::Date,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn datetime(prec: u8) -> Self {
        PreciseType {
            rty: RuntimeType::Datetime,
            detail: Detail::Datetime { prec },
        }
    }

    #[inline]
    pub fn datetime_unknown() -> Self {
        PreciseType {
            rty: RuntimeType::Datetime,
            detail: Detail::None,
        }
    }

    #[inline]
    pub fn interval() -> Self {
        PreciseType {
            rty: RuntimeType::Interval,
            detail: Detail::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Detail {
    None,
    Decimal {
        max_intg: u8,
        max_frac: u8,
    },
    Time {
        prec: u8,
    },
    Datetime {
        prec: u8,
    },
    /// Note: Char and Varchar length is not same as bytes.
    /// It depends on collation, e.g. commonly used utf8mb4
    /// uses at most 4 bytes to store single character.
    Char {
        len: u16,
    },
    Varchar {
        max_len: u16,
    },
}
