use std::io::Write;
use std::sync::mpsc::{Receiver, SendError, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::{anyhow, Context, Result};
use arrow2::array::Array;
use arrow2::datatypes::Field as ArrowField;
use arrow2::datatypes::Schema;
use arrow2::error::ArrowError;
use arrow2::io::parquet::write::{
    to_parquet_schema, write_file, Compression, RowGroupIterator, Version, WriteOptions,
};
use arrow2::record_batch::RecordBatch;
use log::info;

use crate::table::TableField;

pub struct Writer<W> {
    schema: Box<[TableField]>,
    thread: Option<OutThread<W>>,
}

struct OutThread<W> {
    tx: SyncSender<Result<RecordBatch, ArrowError>>,
    thread: JoinHandle<Result<W>>,
}

fn out_thread<W: Write + Send + 'static>(
    mut inner: W,
    schema: &[TableField],
    rx: Receiver<Result<RecordBatch, ArrowError>>,
) -> Result<JoinHandle<Result<W>>> {
    let arrow_schema = Schema::new(
        schema
            .iter()
            .map(|f| ArrowField {
                name: f.name.to_string(),
                data_type: f.kind.to_arrow(),
                nullable: f.nullable,
                dict_id: 0,
                dict_is_ordered: false,
                metadata: None,
            })
            .collect(),
    );

    let write_options = WriteOptions {
        write_statistics: true,
        compression: Compression::Zstd,
        version: Version::V2,
    };
    let encodings = schema.iter().map(|f| f.encoding).collect();
    let parquet_schema = to_parquet_schema(&arrow_schema)?;

    Ok(std::thread::spawn(move || -> Result<W> {
        write_file(
            &mut inner,
            RowGroupIterator::try_new(rx.into_iter(), &arrow_schema, write_options, encodings)?,
            &arrow_schema,
            parquet_schema,
            write_options,
            None,
        )?;
        Ok(inner)
    }))
}

impl<W: Write + Send + 'static> Writer<W> {
    pub fn new(inner: W, schema: &[TableField]) -> Result<Self> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        let thread = out_thread(inner, schema, rx)?;

        Ok(Self {
            schema: schema.to_vec().into_boxed_slice(),
            thread: Some(OutThread { thread, tx }),
        })
    }

    pub fn find_field(&self, name: &str) -> Option<(usize, &TableField)> {
        self.schema.iter().enumerate().find(|(_, f)| f.name == name)
    }

    pub fn submit_batch(&mut self, batch: impl IntoIterator<Item = Arc<dyn Array>>) -> Result<()> {
        let thread = self
            .thread
            .as_mut()
            .ok_or_else(|| anyhow!("already failed"))?;

        let result = RecordBatch::try_from_iter(
            batch
                .into_iter()
                .zip(self.schema.iter())
                .map(|(arr, stat)| (stat.name.to_string(), arr)),
        );

        if let Err(SendError(_)) = thread.tx.send(result) {
            let thread = self.thread.take().expect("it was there a second ago");

            // the read half is gone; this is a fused, finished consumer
            drop(thread.tx);
            thread
                .thread
                .join()
                .expect("thread panic")
                .with_context(|| anyhow!("file writer had failed"))?;
        }

        Ok(())
    }

    pub fn finish(mut self) -> Result<W> {
        let thread = self
            .thread
            .take()
            .ok_or_else(|| anyhow!("already failed"))?;

        info!("finishing...");
        drop(thread.tx);
        match thread.thread.join() {
            Ok(res) => res,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}
