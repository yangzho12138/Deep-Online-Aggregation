use std::sync::mpsc::Receiver;

use pyo3::prelude::*;

#[pyclass]
pub struct Mydata {
    pub iter: Receiver<String>,
}

#[pymethods]
impl Mydata {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<Self>) -> Option<String> {
        slf.iter.recv().ok()
    }
}