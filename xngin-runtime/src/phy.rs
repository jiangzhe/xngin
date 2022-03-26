/// Phy is the physical
pub struct Phy {
    pub kind: PhyKind,
    pub input: Vec<Phy>,
    pub output: Vec<Phy>,
}

pub enum PhyKind {}
