use std::io::Write;

use anyhow::Result;
use log::info;

use crate::{Table, TableField, Writer};

pub struct Packer<W> {
    writer: Writer<W>,
    table: Table,
}

impl<W: Write + Send + 'static> Packer<W> {
    pub fn new(inner: W, schema: &[TableField]) -> Result<Self> {
        Ok(Self {
            writer: Writer::new(vec![inner], schema)?,
            table: Table::with_capacity(&schema.iter().map(|f| f.kind).collect::<Vec<_>>(), 0),
        })
    }

    pub fn table(&mut self) -> &mut Table {
        &mut self.table
    }

    pub fn find_field(&self, name: &str) -> Option<(usize, &TableField)> {
        self.writer.find_field(name)
    }

    pub fn consider_flushing(&mut self) -> Result<()> {
        self.table.check_consistent()?;

        if self.table.mem_estimate() > 512 * 1024 * 1024 || self.table.rows() > 100_000 {
            self.flush()?;
        }

        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        let rows = self.table.rows();
        if 0 == rows {
            return Ok(());
        }

        // update the memory estimate, and check consistent
        self.table.finish_bulk_push()?;
        let mem_estimate = self.table.mem_estimate();

        info!(
            "submitting row group ({} rows, ~{}MB, ~{}bytes/row)",
            rows,
            mem_estimate / 1024 / 1024,
            mem_estimate / rows
        );

        let batch = self.table.take_batch();

        self.writer.submit_batch(batch)?;

        Ok(())
    }

    pub fn finish(mut self) -> Result<W> {
        self.flush()?;
        Ok(self.writer.finish()?.pop().expect("exactly one"))
    }
}
