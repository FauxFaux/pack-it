#![feature(try_blocks)]

mod erratum;
mod mem;
mod packer;
pub mod repack;
mod table;
mod write;

pub use crate::mem::MemUsage;
pub use crate::packer::Packer;
pub use crate::table::Kind;
pub use crate::table::Table;
pub use crate::table::TableField;
pub use crate::table::VarArray;
pub use crate::write::Writer;
