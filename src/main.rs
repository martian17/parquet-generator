use anyhow::{Result, bail};
use arrow::array::{UInt16Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, mpsc};

use rand::rngs::StdRng;
use rand::{Rng,SeedableRng};

/// Write a series of Parquet files to disk containing the data from the input queue.
///
/// For write efficiency and ease in handling large volumes of data, we batch writes to Parquet files in chunks of about 200 MiB (as recommended in [this discussion](https://github.com/apache/arrow/issues/13142)), and then rotate to a new file approximately every 2 GiB. Rows are assumed to contain about 80 bits of data each; ignoring metadata overhead and compression, this means that a 2 GiB file can hold approximately 214,700,000 rows. For simplicity, we set the default size limit for each file to 200,000,000 rows, and default chunk size to 20,000,000.
// pub struct TimeTagStreamParquetWriter {
//     // The maximum number of total rows (records) that should be
//     // collected before writing to disk.
//     max_chunk_rows: usize,
//     // The maximum number of total rows (records) that should be
//     // allowed per file.
//     max_file_rows: usize,
// }
// 
// impl TimeTagStreamParquetWriter {
//     #[must_use]
//     pub fn new() -> TimeTagStreamParquetWriter {
//         TimeTagStreamParquetWriter {
//             max_chunk_rows: 20_000_000,
//             max_file_rows: 200_000_000,
//         }
//     }
//     pub fn write(...) ... {...}
// }


pub struct NormalizedTimeTag {
    pub channel_id: u16,

    /// The time tag, in picoseconds, counting up from the start of the measurement.
    pub time_tag_ps: u64,
}

// write mock time tags to parquet files
pub fn write_time_tags(
    time_tags: Vec<NormalizedTimeTag>,
    output_dir: &Path,
    name: &str,
) -> Result<()> {
    let max_chunk_rows: usize = 20_000_000;
    let max_file_rows: usize = 200_000_000;
    if !output_dir.is_dir() {
        bail!(
            "Requested output path {} is not a directory.",
            output_dir.display()
        );
    }
    let fields = vec![
        Field::new("channel", DataType::UInt16, false),
        Field::new("time_tag", DataType::UInt64, false),
    ];
    let schema: Arc<Schema> = Schema::new(fields).into();

    let max_chunk_count = max_file_rows / max_chunk_rows;
    let file_timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");

    let mut total_files = 1;
    let initial_file = File::create_new(
        output_dir.join(format!("{file_timestamp}_{name}_{total_files:0>4}.parquet")),
    )?;
    let mut arrow_writer = ArrowWriter::try_new(initial_file, schema.clone(), None)?;
    let mut channel_array_builder = UInt16Array::builder(max_chunk_rows);
    let mut time_tag_array_builder = UInt64Array::builder(max_chunk_rows);
    let mut array_length = 0;
    let mut chunk_count = 0;
    // for rx_batch in rx_channel {
    //     for event in rx_batch {
    //         array_length += 1;
    //         channel_array_builder.append_value(event.channel_id);
    //         time_tag_array_builder.append_value(event.time_tag_ps);
    //     }
    for event in time_tags {
        array_length += 1;
        channel_array_builder.append_value(event.channel_id);
        time_tag_array_builder.append_value(event.time_tag_ps);

        if array_length >= max_chunk_rows {
            // write current batch into current file
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(channel_array_builder.finish()),
                    Arc::new(time_tag_array_builder.finish()),
                ],
            )?;
            arrow_writer.write(&batch)?;
            array_length = 0;
            chunk_count += 1;
        }

        if chunk_count > max_chunk_count {
            // close and replace file
            arrow_writer.close()?;
            chunk_count = 0;
            total_files += 1;

            let new_file = File::create_new(
                output_dir.join(format!("{file_timestamp}_{name}_{total_files:0>4}.parquet")),
            )?;
            arrow_writer = ArrowWriter::try_new(new_file, schema.clone(), None)?;
        }
    }

    // write any remaining data
    if array_length > 0 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(channel_array_builder.finish()),
                Arc::new(time_tag_array_builder.finish()),
            ],
        )?;
        arrow_writer.write(&batch)?;
    }
    arrow_writer.close()?;

    Ok(())
}

fn pico_seconds (seconds: u64) -> u64 {
    return 1_000_000_000_000u64*seconds;
}

fn pico_minutes (minutes: u64) -> u64 {
    return 1_000_000_000_000u64*60*minutes;
}

fn pico_fiber_centimeters (centimeters: u64) -> u64 {
    // approx. amount of time it takes for the light to propagate through fiber
    return 50u64*centimeters;
}

fn main() {
    let mut time_tags = Vec::<NormalizedTimeTag>::new();
    let mut r = StdRng::seed_from_u64(42);

    let simulation_range = 0..pico_minutes(7);
    let simulation_offset = pico_seconds(1);// prevents overflow
    let sample_range = pico_minutes(1)..pico_minutes(6);
    let multiharp_160_jitter: std::ops::Range<i64> = -15..15;// acc. specs 25 ps
    for _ in 0..1000000 {
        let mut pico0: u64 = r.random_range(simulation_range.clone()) + simulation_offset;
        let delta: u64 = (pico_fiber_centimeters(10) as i64 + r.random_range(multiharp_160_jitter.clone())) as u64;

        let tag0 = NormalizedTimeTag{
            channel_id: 0,
            time_tag_ps: pico0,
        };
        let tag1 = if r.random_range(0..100) < 50 {
            NormalizedTimeTag{
                channel_id: 1,
                time_tag_ps: pico0 + delta,
            }
        }else{
            let mut pico1: u64 = r.random_range(simulation_range.clone()) + simulation_offset;
            NormalizedTimeTag{
                channel_id: 1,
                time_tag_ps: pico1,
            }
        };
        if sample_range.start < tag0.time_tag_ps && tag0.time_tag_ps < sample_range.end {
            time_tags.push(tag0);
        }
        if sample_range.start < tag1.time_tag_ps && tag1.time_tag_ps < sample_range.end {
            time_tags.push(tag1);
        }
    }
    
    time_tags.sort_by_key(|tag| tag.time_tag_ps);
    
    write_time_tags(
        time_tags,// time_tags: Vec<NormalizedTimeTag>,
        Path::new("./"),// output_dir: &Path,
        "simulation-1",// name: &str,
    ).unwrap_or_else(|err|{
            panic!("{:?}", err);
    })
}



// stub
// fn main() {
//     write_time_tags(
//         vec![
//             NormalizedTimeTag {
//                 channel_id: 0,
//                 /// The time tag, in picoseconds, counting up from the start of the measurement.
//                 time_tag_ps: 100,
//             },
//             NormalizedTimeTag {
//                 channel_id: 1,
//                 time_tag_ps: 200,
//             },
//             NormalizedTimeTag {
//                 channel_id: 0,
//                 time_tag_ps: 181,
//             },
//             NormalizedTimeTag {
//                 channel_id: 1,
//                 time_tag_ps: 201,
//             },
//             NormalizedTimeTag {
//                 channel_id: 0,
//                 time_tag_ps: 210,
//             },
//         ],// time_tags: Vec<NormalizedTimeTag>,
//         Path::new("./"),// output_dir: &Path,
//         "simulation-1",// name: &str,
//     ).unwrap_or_else(|err|{
//             panic!("{:?}", err);
//     })
// }
