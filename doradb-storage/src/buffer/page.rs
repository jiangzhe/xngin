pub const PAGE_SIZE: usize = 64 * 1024;
pub type Page = [u8; PAGE_SIZE];
pub type PageID = u64;
pub const INVALID_PAGE_ID: PageID = !0;
pub type LSN = u64;

pub trait PageOps: Sized {
    /// Initialize page and returns mutable reference of 
    /// in-memory representation.
    fn init(page: &mut Page, height: usize) -> &mut Self;

    fn cast(page: &Page) -> &Self {
        unsafe { &*(page as *const _ as *const Self) }
    }

    /// convert page to Self.
    fn cast_mut(page: &mut Page) -> &mut Self {
        unsafe { &mut *(page as *mut _ as *mut Self) }
    }
}