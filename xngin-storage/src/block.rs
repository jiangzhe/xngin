use crate::codec::Codec;
use crate::psma::PSMA;
use xngin_datatype::PreciseType;

/// Block collects multiple tuples and aggregate synopses for analytical query.
pub struct Block {
    pub n_tuples: usize,
    pub attrs: Vec<Attr>,
}

pub struct Attr {
    pub pty: PreciseType,
    pub codec: Codec,
    pub psma: Option<PSMA>,
}
