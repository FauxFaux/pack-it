#![feature(try_blocks)]

mod mem;
pub mod repack;
mod table;
mod write;

pub use crate::mem::MemUsage;
pub use crate::table::Kind;
pub use crate::table::Table;
pub use crate::table::VarArray;
pub use crate::write::Packer;
pub use crate::write::TableField;
pub use crate::write::Writer;
