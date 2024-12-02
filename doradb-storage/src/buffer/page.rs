pub const PAGE_SIZE: usize = 64 * 1024;
pub type Page = [u8; PAGE_SIZE];
pub type PageID = u64;
pub const INVALID_PAGE_ID: PageID = !0;
pub type LSN = u64;
