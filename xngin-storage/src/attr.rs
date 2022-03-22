use crate::codec::Codec;
use xngin_common::psma::PSMA;
use xngin_datatype::PreciseType;

pub struct Attr {
    pub ty: PreciseType,
    pub codec: Codec,
    pub psma: Option<PSMA>,
}

impl Attr {
    #[inline]
    pub fn to_owned(&self) -> Self {
        Attr {
            ty: self.ty,
            codec: self.codec.to_owned(),
            psma: None,
        }
    }
}
