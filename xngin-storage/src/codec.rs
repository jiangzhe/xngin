use xngin_common::array::FFIArray;
use xngin_common::bitmap::FFIBitmap;

/// Codec of attributes.
/// The design of codec is heavily inspired by paper "Data Blocks: Hybrid OLTP
/// and OLAP on Compressed Storage using both Vectorization and Compilation".
/// The main idea is to make the codec byte-addressable, consistent with
/// in-memory and persistent format.
/// Byte-addressable is to make the point search efficient, especially without
/// decompression.
/// the consistency between memory and disk requires little effort on serialization
/// and deserialization.
pub enum Codec {
    Single(SingleCodec),
    Flat(FlatCodec),
}

impl Codec {
    #[inline]
    pub fn kind(&self) -> CodecKind {
        match self {
            Codec::Single(_) => CodecKind::Single,
            Codec::Flat(_) => CodecKind::Flat,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CodecKind {
    Single,
    Flat,
    Dict,
    Trunc,
    String,
}

/// Single codec encodes single value as bytes in case all
/// values of that attribute are identical.
pub struct SingleCodec(Vec<u8>);

/// Flat codec represents uncompressed data of one attribute.
/// Ususally the data type is not
pub struct FlatCodec {
    pub validity: Option<FFIBitmap>,
    pub data: FFIArray,
}
