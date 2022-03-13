use crate::codec::Codec;
use xngin_common::psma::PSMA;
use xngin_datatype::{PreciseType, I32};

pub struct Attr {
    pub pty: PreciseType,
    pub codec: Codec,
    pub psma: Option<PSMA>,
}

impl From<Vec<i32>> for Attr {
    #[inline]
    fn from(src: Vec<i32>) -> Self {
        let codec = Codec::from(src);
        let psma = match &codec {
            Codec::Single(_) => None,
            Codec::Flat(f) => {
                let (validity, data) = f.view();
                PSMA::build_from_i32s(validity, data)
            }
        };
        Attr {
            pty: *I32,
            codec,
            psma,
        }
    }
}
