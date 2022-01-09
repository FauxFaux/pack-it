#![feature(try_blocks)]

pub mod repack;
mod table;
mod write;

pub use crate::table::Kind;
pub use crate::table::VarArray;
pub use crate::write::TableField;
pub use crate::write::Writer;
