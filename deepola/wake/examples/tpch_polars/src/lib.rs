use myiter::Mydata;
use pyo3::prelude::*;
use polars::prelude::DataFrame;
use crate::tpch::q1::query;
use wake::graph::ExecutionService;
use wake::graph::*;
use std::thread;
use std::sync::mpsc;
use crate::utils::run_query;

pub mod tpch;
pub mod utils;
pub mod myiter;

#[pyfunction]
fn run_thread(
    query_no: &str,
    scale: usize,
    data_directory: &str,
) -> PyResult<Mydata> {
    let (sender, receiver) = mpsc::channel::<String>();
    let res_r = Mydata { iter: receiver };
    let mut output_reader = NodeReader::empty();
    let qService = get_query_service(query_no, scale, data_directory, &mut output_reader);

    thread::spawn(move || {
        sender.send(run_query(&mut qService, &mut output_reader)).unwrap();
    });

    Ok(res_r)
}

pub fn get_query_service(
    query_no: &str,
    scale: usize,
    data_directory: &str,
    output_reader: &mut NodeReader<DataFrame>,
) -> ExecutionService<DataFrame> {
    let table_input = utils::load_tables(data_directory, scale);
    // TODO: UNCOMMENT THE MATCH STATEMENTS BELOW AS YOU IMPLEMENT THESE QUERIES.
    let query_service = match query_no {
        "q1" => query(table_input, output_reader),
        _ => panic!("Invalid Query Parameter"),
    };
    query_service
}

/// A Python module implemented in Rust. The name of this function must match
/// the `lib.name` setting in the `Cargo.toml`, else Python will not be able to
/// import the module.
#[pymodule]
fn tpch_polars(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_thread, m)?)?;
    Ok(())
}