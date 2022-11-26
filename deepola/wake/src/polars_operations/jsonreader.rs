use std::fs::File;
use polars::series::Series;
use polars::prelude::*;

use crate::data::*;
use crate::graph::ExecutionNode;
use crate::processor::StreamProcessor;
use std::time::SystemTime;

pub struct JsonReaderBuilder {
    infer_schema_len: Option<usize>,
    batch_size: usize,
    projection: Option<Vec<String>>,
    // not sure if needed
    // schema: Option<ArrowSchema>,
    // json_format: JsonFormat,
}

impl Default for JsonReaderBuilder {
    fn default() -> Self {
        JsonReaderBuilder {
            infer_schema_len: Some(3),
            batch_size: 3,
            projection: Option::None,
            // not sure if needed
            // schema: Option<ArrowSchema>,
            // json_format: JsonFormat,
        }
    }
}

impl JsonReaderBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    // pub fn with_schema(&mut self, schema: Option<ArrowSchema>) -> &mut Self {
    //     self.schema = schema;
    //     self
    // }

    // same as column names for other readers
    pub fn with_projection(&mut self, projection: Option<Vec<usize>>) -> &mut Self {
        self.projection = projection;
        self
    }

    pub fn build(&self) -> ExecutionNode<DataFrame> {
        let data_processor = JsonReader::new(
            self.projection.clone(),
            // self.projected_cols.clone(),
        );
        ExecutionNode::<DataFrame>::new(Box::new(data_processor), 1)
    }
}

/// A custom SetProcessor<Series> type for reading parquet files.
struct JsonReader {
    infer_schema_len: Option<usize>,
    batch_size: usize,
    projection: Option<Vec<String>>,
    // not sure if needed
    // schema: Option<ArrowSchema>,
    // json_format: JsonFormat,
}

/// A factory method for creating the custom SetProcessor<Series> type for
/// reading parquet files
impl JsonReader {
    pub fn new(
        infer_schema_len: Option<usize>,
        batch_size: usize,
        projection: Option<Vec<String>>,
        // not sure if needed
        // schema: Option<ArrowSchema>,
        // json_format: JsonFormat,
    ) -> Self {
        JsonReader {
            infer_schema_len,
            batch_size,
            projection,
            // not sure if needed
            schema,
            json_format,
        }
    }

    fn dataframe_from_filename(&self, filename: &str) -> DataFrame {
        /* TODO: NEED TO IMPLEMENT THIS */
        /* Refer to the implementation of `dataframe_from_filename` in `csvreader.rs` */
        log::info!("Begin ReadFile Json: {:?}", SystemTime::now());
        println!("{:?}",SystemTime::now());
        // need to change
        let f = File::open(filename).unwrap();
        let mut reader = polars::prelude::JsonReader::new(f);
        // if self.projection.is_some() {
        //     reader = reader.with_projection(self.projection.clone());
        // }
        let mut df = reader.finish().unwrap();

        log::info!("End ReadFile Json: {:?}", SystemTime::now());
        println!("{:?}",SystemTime::now());

        if self.projection.is_some(){
            if let Some(a) = &self.projection{
                df.set_column_names(a).unwrap();
            }
        }
        df
    }
}

impl StreamProcessor<DataFrame> for JsonReader {
    fn process_stream(
        &self,
        input_stream: crate::channel::MultiChannelReader<DataFrame>,
        output_stream: crate::channel::MultiChannelBroadcaster<DataFrame>,
    ) {
        loop {
            let channel_seq = 0;
            let message = input_stream.read(channel_seq);
            match message.payload() {
                Payload::EOF => {
                    output_stream.write(message);
                    break;
                }
                Payload::Signal(_) => break,
                Payload::Some(dblock) => {
                    for series in dblock.data().iter() {
                        // This must be a length-one Polars series containing
                        // file names in its rows
                        let rows = series.utf8().unwrap();

                        // each file name produces multiple Series (each is a column)
                        rows.into_iter().for_each(|filename| {
                            let df = self.dataframe_from_filename(filename.unwrap());
                            let message = DataMessage::from(DataBlock::from(df));
                            output_stream.write(message);
                        });
                    }
                }
            }
        }
    }
}
