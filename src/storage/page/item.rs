use zerocopy_derive::{IntoBytes, FromBytes, Immutable, KnownLayout};


pub mod item_id_flags {
    pub const LP_UNUSED: u8 = 0;
    pub const LP_NORMAL: u8 = 1;
    pub const LP_REDIRECT: u8 = 2;
    pub const LP_DEAD: u8 = 3;
}

#[repr(transparent)]
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout, Debug, Copy, Clone)]
/// ItemIdData encodes the offset, length, and flags for a tuple in a page.
/// It is stored in the line pointer array of a page header and points to the actual tuple data in the page.
pub struct ItemIdData(pub u32);

impl ItemIdData {
    // First 15 bits: offset to tuple data, next 2 bits: flags, last 15 bits: length of tuple data
    pub(crate) fn new(off: u16, len: u16, flags: u8) -> Self {
        Self((off as u32 & 0x7FFF) | ((flags as u32 & 0x3) << 15) | ((len as u32 & 0x7FFF) << 17))
    }
    pub(crate) fn lp_off(&self) -> u16 { (self.0 & 0x7FFF) as u16 }
    pub(crate) fn lp_flags(&self) -> u8 { ((self.0 >> 15) & 0x3) as u8 }
    pub(crate) fn lp_len(&self) -> u16 { (self.0 >> 17) as u16 }
    pub(crate) fn set_lp_off(&mut self, off: u16) { self.0 = (self.0 & !0x7FFF) | (off as u32 & 0x7FFF) }
    /// Possible flags: LP_UNUSED, LP_NORMAL, LP_REDIRECT, LP_DEAD.
    pub(crate) fn set_lp_flags(&mut self, flags: u8) { self.0 = (self.0 & !(0x3 << 15)) | ((flags as u32 & 0x3) << 15) }
    /// Length of the tuple data in bytes.
    pub(crate) fn set_lp_len(&mut self, len: u16) { self.0 = (self.0 & !(0x7FFF << 17)) | ((len as u32 & 0x7FFF) << 17) }

    pub(crate) fn is_unused(&self) -> bool { self.lp_flags() == item_id_flags::LP_UNUSED }
    pub(crate) fn is_normal(&self) -> bool { self.lp_flags() == item_id_flags::LP_NORMAL }
    pub(crate) fn is_redirect(&self) -> bool { self.lp_flags() == item_id_flags::LP_REDIRECT }
    pub(crate) fn is_dead(&self) -> bool { self.lp_flags() == item_id_flags::LP_DEAD }
}

pub trait PageItem {
    fn len(&self) -> usize;
    fn serialize_into(&self, dest: &mut [u8]);
}

impl PageItem for &[u8] {
    fn len(&self) -> usize { (*self).len() }
    fn serialize_into(&self, dest: &mut [u8]) {
        dest.copy_from_slice(self);
    }
}