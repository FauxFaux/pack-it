use std::io::Write;
use std::sync::Arc;
use std::thread::JoinHandle;

use crate::erratum::join;
use anyhow::{anyhow, bail, Result};
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::datatypes::{Field as ArrowField, Metadata};
use arrow2::error::Error as ArrowError;
use arrow2::io::parquet::write::{
    CompressionOptions, FileWriter, RowGroupIterator, Version, WriteOptions,
};
use crossbeam_channel::{SendError, Sender};
use log::info;

use crate::table::TableField;

pub struct Writer<W> {
    schema: Box<[TableField]>,
    threads: Vec<JoinHandle<Result<W>>>,
    tx: Option<Sender<Result<Chunk<Arc<dyn Array>>, ArrowError>>>,
}

fn out_thread<W: Write + Send + 'static>(
    mut inner: W,
    schema: &[TableField],
    rx: impl IntoIterator<Item = Result<Chunk<Arc<dyn Array>>, ArrowError>> + Send + 'static,
) -> Result<JoinHandle<Result<W>>> {
    let arrow_schema = Schema::from(
        schema
            .iter()
            .map(|f| ArrowField {
                name: f.name.to_string(),
                data_type: f.kind.to_arrow(),
                is_nullable: f.nullable,
                metadata: Metadata::default(),
            })
            .collect::<Vec<_>>(),
    );

    let write_options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Zstd(None),
        version: Version::V2,
        data_pagesize_limit: None,
    };
    let encodings = schema.iter().map(|f| vec![f.encoding]).collect();

    Ok(std::thread::spawn(move || -> Result<W> {
        let rg_iter =
            RowGroupIterator::try_new(rx.into_iter(), &arrow_schema, write_options, encodings)?;
        let mut writer = FileWriter::try_new(&mut inner, arrow_schema, write_options)?;

        for rg in rg_iter {
            let row_group = rg?;
            writer.write(row_group)?;
        }

        writer.end(None)?;
        Ok(inner)
    }))
}

impl<W: Write + Send + 'static> Writer<W> {
    pub fn new(
        inner: impl IntoIterator<Item = W, IntoIter = impl Iterator<Item = W> + ExactSizeIterator>,
        schema: &[TableField],
    ) -> Result<Self> {
        let inner = inner.into_iter();

        let (tx, rx) = crossbeam_channel::bounded(inner.len());

        let threads = inner
            .into_iter()
            .map(|inner| out_thread(inner, schema, rx.clone()))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            schema: schema.to_vec().into_boxed_slice(),
            threads,
            tx: Some(tx),
        })
    }

    pub fn find_field(&self, name: &str) -> Option<(usize, &TableField)> {
        self.schema.iter().enumerate().find(|(_, f)| f.name == name)
    }

    pub fn submit_batch(&mut self, batch: impl IntoIterator<Item = Arc<dyn Array>>) -> Result<()> {
        let result = Chunk::try_new(batch.into_iter().collect())?;

        let tx = self
            .tx
            .as_mut()
            .ok_or_else(|| anyhow!("previously failed"))?;

        if let Err(SendError(_)) = tx.send(Ok(result)) {
            // all of the writers have failed, so we need to die
            // (this doesn't catch the case where one writer has died)
            drop(self.tx.take());

            // this should fail
            join_all(&mut self.threads)?;

            bail!("all of the threads have gone, but none have bothered to tell us why");
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<Vec<W>> {
        if self.threads.is_empty() {
            bail!("had previously failed");
        }

        info!("finishing...");
        drop(self.tx);

        join_all(&mut self.threads)
    }
}

fn join_all<T>(threads: &mut Vec<JoinHandle<Result<T>>>) -> Result<Vec<T>> {
    let mut ret = Vec::with_capacity(threads.len());
    while let Some(thread) = threads.pop() {
        ret.push(join(thread)?);
    }
    Ok(ret)
}
