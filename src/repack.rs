use std::io::{Read, Seek, Write};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow2::array::{
    Array, BooleanArray, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array,
    PrimitiveArray, TryExtend, Utf8Array,
};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::read;
use arrow2::io::parquet::read::RowGroupMetaData;
use arrow2::io::parquet::write::Encoding;
use log::info;

use crate::table::VarArray;
use crate::{Kind, Packer, TableField};

#[derive(Clone)]
pub struct OutField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,

    pub encoding: Encoding,
}

// struct Transform {
//     input: String,
//     output: OutField,
//     func: Box<dyn FnMut(Box<dyn Array>, &mut Table, usize) -> Result<()>>,
// }

pub struct Split {
    pub output: Vec<OutField>,
    pub func: Box<dyn Send + FnMut(Arc<dyn Array>, &mut [&mut VarArray]) -> Result<()>>,
}

pub enum Action {
    ErrorOut,
    Drop,
    Copy,
    // Transform(Transform),
    Split(Split),
}

pub struct Op {
    pub input: String,
    pub action: Action,
}

pub struct Repack {
    pub ops: Vec<Op>,
}

fn find_field<'f>(schema: &'f Schema, name: &str) -> Option<(usize, &'f Field)> {
    schema
        .fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == name)
}

pub enum LoopDecision {
    Include,
    Skip,
    Break,
}

pub fn transform<W: Write + Send + 'static>(
    mut f: impl Read + Seek,
    out: W,
    repack: &mut Repack,
    mut rg_filter: impl FnMut(usize, &RowGroupMetaData) -> LoopDecision,
) -> Result<W> {
    let metadata = read::read_metadata(&mut f)?;
    let in_schema = read::infer_schema(&metadata)?;

    let out_schema = repack
        .ops
        .iter()
        .flat_map(|op| -> Vec<Result<OutField>> {
            match &op.action {
                Action::Drop | Action::ErrorOut => Vec::new(),
                Action::Copy => vec![
                    try {
                        let (_, x) = find_field(&in_schema, &op.input)
                            .ok_or_else(|| anyhow!("field has gone missing?"))?;
                        OutField {
                            name: x.name.to_string(),
                            data_type: x.data_type.clone(),
                            nullable: x.is_nullable,
                            encoding: Encoding::Plain,
                        }
                    },
                ],

                Action::Split(split) => split.output.iter().cloned().map(Ok).collect(),
            }
        })
        .collect::<Result<Vec<OutField>>>()?;

    let table_schema = out_schema
        .iter()
        .map(|v| -> Result<TableField> {
            Ok(TableField {
                name: v.name.to_string(),
                kind: Kind::from_arrow(&v.data_type)
                    .with_context(|| anyhow!("converting {:?} to a Kind", v.name))?,
                nullable: false,
                encoding: Encoding::Plain,
            })
        })
        .collect::<Result<Vec<_>>>()
        .with_context(|| anyhow!("generating an internal schema for the output"))?;

    let mut writer = Packer::new(out, &table_schema)?;

    for (rg, rg_meta) in metadata.row_groups.iter().enumerate() {
        info!(
            "handling rg {}/{} ({} rows)",
            rg,
            metadata.row_groups.len(),
            rg_meta.num_rows()
        );

        match rg_filter(rg, rg_meta) {
            LoopDecision::Include => (),
            LoopDecision::Skip => continue,
            LoopDecision::Break => break,
        };

        for op in &mut repack.ops {
            let (_field, field_meta) = find_field(&in_schema, &op.input)
                .ok_or_else(|| anyhow!("looking up input field {:?}", op.input))?;
            let col = read::read_columns(&mut f, rg_meta.columns(), &field_meta.name)?;
            let des = read::to_deserializer(
                col,
                field_meta.clone(),
                rg_meta
                    .num_rows()
                    .try_into()
                    .expect("row count fits in memory"),
                None,
            )?;
            let arr = Arc::clone(&des.collect::<Result<Vec<_>, _>>()?[0]);

            match &mut op.action {
                Action::ErrorOut => bail!("asked to error out after loading {:?}", field_meta.name),
                Action::Drop => unimplemented!("drop"),
                Action::Copy => {
                    let (output, _) = writer.find_field(&op.input).expect("created above");

                    let output = writer.table().get(output);

                    if let Some(output) = output.downcast_mut::<MutableUtf8Array<i32>>() {
                        output
                            .try_extend(
                                arr.as_any()
                                    .downcast_ref::<Utf8Array<i32>>()
                                    .expect("input=output")
                                    .iter(),
                            )
                            .with_context(|| {
                                anyhow!("copying {} rows of {:?}", metadata.num_rows, op.input)
                            })?;
                    } else if let Some(output) = output.downcast_mut::<MutablePrimitiveArray<i64>>()
                    {
                        output.extend(
                            arr.as_any()
                                .downcast_ref::<PrimitiveArray<i64>>()
                                .expect("input=output")
                                .iter()
                                .map(|v| v.map(|x| *x)),
                        );
                    } else if let Some(output) = output.downcast_mut::<MutablePrimitiveArray<i32>>()
                    {
                        output.extend(
                            arr.as_any()
                                .downcast_ref::<PrimitiveArray<i32>>()
                                .expect("input=output")
                                .iter()
                                .map(|v| v.map(|x| *x)),
                        );
                    } else if let Some(output) = output.downcast_mut::<MutableBooleanArray>() {
                        output.extend(
                            arr.as_any()
                                .downcast_ref::<BooleanArray>()
                                .expect("input=output")
                                .iter(),
                        );
                    } else if let Some(output) = output.downcast_mut::<MutablePrimitiveArray<f64>>()
                    {
                        output.extend(
                            arr.as_any()
                                .downcast_ref::<PrimitiveArray<f64>>()
                                .expect("input=output")
                                .iter()
                                .map(|v| v.map(|x| *x)),
                        );
                    } else {
                        bail!(
                            "copy for {:?} columns ({:?})",
                            field_meta.data_type,
                            field_meta.name
                        )
                    }
                }
                Action::Split(s) => {
                    let fields: Vec<usize> = s
                        .output
                        .iter()
                        .map(|f| {
                            writer
                                .find_field(&f.name)
                                .expect("created based on input")
                                .0
                        })
                        .collect();
                    (s.func)(arr, &mut writer.table().get_many(&fields))?;
                }
            }
        }

        writer.table().finish_bulk_push()?;
        writer.consider_flushing()?;
    }

    writer.finish()
}
