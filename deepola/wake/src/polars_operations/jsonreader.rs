use std::fs::File;
use polars::series::Series;
use polars::prelude::*;

use crate::data::*;
use crate::graph::ExecutionNode;
use crate::processor::StreamProcessor;
use std::time::SystemTime;
// added
use std::fs;
use std::io::Cursor;

pub struct JsonReaderBuilder {

    column_names: Option<Vec<String>>,
    projected_cols: Option<Vec<String>>,

}

impl Default for JsonReaderBuilder {
    fn default() -> Self {
        JsonReaderBuilder {
            column_names: Option::None,
            projected_cols: Option::None,
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
    pub fn column_names(&mut self, column_names: Option<Vec<String>>) -> &mut Self {
        self.column_names = column_names;
        self
    }

    pub fn projected_cols(&mut self, projected_cols: Option<Vec<String>>) -> &mut Self {
        self.projected_cols = projected_cols;
        self
    }
    pub fn build(&self) -> ExecutionNode<DataFrame> {
        let data_processor = JsonReader::new(
            self.column_names.clone(),
            self.projected_cols.clone(),
        );
        ExecutionNode::<DataFrame>::new(Box::new(data_processor), 1)
    }

}

/// A custom SetProcessor<Series> type for reading parquet files.
struct JsonReader {
    column_names: Option<Vec<String>>,
    projected_cols: Option<Vec<String>>,
}

/// A factory method for creating the custom SetProcessor<Series> type for
/// reading parquet files
impl JsonReader {
    pub fn new(
        column_names: Option<Vec<String>>,
        projected_cols: Option<Vec<String>>,

    ) -> Self {
        JsonReader {
            column_names,
            projected_cols,
        }
    }

    fn dataframe_from_filename(&self, filename: &str) -> DataFrame {
        /* TODO: NEED TO IMPLEMENT THIS */
        /* Refer to the implementation of `dataframe_from_filename` in `csvreader.rs` */
        log::info!("Begin ReadFile Json: {:?}", SystemTime::now());
        println!("{:?}",SystemTime::now());
        // println!("{}",filename);
        let f = File::open(filename).unwrap();
        let contents = fs::read_to_string(filename).expect("Should have been able to read the file");
        
        // let mut contents_cp = r#"{"a":1, "b":2.0, "c":false, "d":"4"}
        // //! {"a":-10, "b":-3.5, "c":true, "d":"4"}
        // //! {"a":2, "b":0.6, "c":false, "d":"text"}
        // //! {"a":1, "b":2.0, "c":false, "d":"4"}
        // //! {"a":7, "b":-3.5, "c":true, "d":"4"}
        // //! {"a":1, "b":0.6, "c":false, "d":"text"}
        // //! {"a":1, "b":2.0, "c":false, "d":"4"}
        // //! {"a":5, "b":-3.5, "c":true, "d":"4"}
        // //! {"a":1, "b":0.6, "c":false, "d":"text"}
        // //! {"a":1, "b":2.0, "c":false, "d":"4"}
        // //! {"a":1, "b":-3.5, "c":true, "d":"4"}
        // //! {"a":1, "b":0.6, "c":false, "d":"text"}"#;
        // println!("{}",contents);
        let file = Cursor::new(contents);
        let mut reader = polars::prelude::JsonReader::new(f);
        // println!("--------------df");
        if self.projected_cols.is_some() {
            reader = reader.with_projection(self.projected_cols.clone());
        }
        let mut df = reader
        .finish()
        .unwrap();
       
        log::info!("End ReadFile Json: {:?}", SystemTime::now());
        println!("{:?}",SystemTime::now());

        if self.column_names.is_some(){
            if let Some(a) = &self.column_names{
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
