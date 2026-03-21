/// PyO3 native extension exposing BSATN encode/decode primitives.
///
/// All decode functions return a (value, new_offset) tuple so callers can
/// step through a buffer sequentially without copying slices.
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

// ── helpers ──────────────────────────────────────────────────────────────────

fn buf_slice(buf: &[u8], offset: usize, len: usize) -> PyResult<&[u8]> {
    buf.get(offset..offset + len).ok_or_else(|| {
        PyValueError::new_err(format!(
            "buffer too short: need {} bytes at offset {}, have {}",
            len,
            offset,
            buf.len()
        ))
    })
}

// ── bool ─────────────────────────────────────────────────────────────────────

#[pyfunction]
fn encode_bool(py: Python<'_>, v: bool) -> PyObject {
    PyBytes::new_bound(py, &[v as u8]).into()
}

#[pyfunction]
fn decode_bool(buf: &[u8], offset: usize) -> PyResult<(bool, usize)> {
    let b = buf_slice(buf, offset, 1)?[0];
    match b {
        0 => Ok((false, offset + 1)),
        1 => Ok((true, offset + 1)),
        other => Err(PyValueError::new_err(format!(
            "invalid bool byte {other:#04x} at offset {offset}"
        ))),
    }
}

// ── unsigned integers ─────────────────────────────────────────────────────────

macro_rules! encode_uint {
    ($name:ident, $ty:ty) => {
        #[pyfunction]
        fn $name(py: Python<'_>, v: $ty) -> PyObject {
            PyBytes::new_bound(py, &v.to_le_bytes()).into()
        }
    };
}

macro_rules! decode_uint {
    ($name:ident, $ty:ty) => {
        #[pyfunction]
        fn $name(buf: &[u8], offset: usize) -> PyResult<($ty, usize)> {
            const N: usize = std::mem::size_of::<$ty>();
            let bytes: [u8; N] = buf_slice(buf, offset, N)?.try_into().unwrap();
            Ok((<$ty>::from_le_bytes(bytes), offset + N))
        }
    };
}

encode_uint!(encode_u8, u8);
encode_uint!(encode_u16, u16);
encode_uint!(encode_u32, u32);
encode_uint!(encode_u64, u64);
encode_uint!(encode_u128, u128);

decode_uint!(decode_u8, u8);
decode_uint!(decode_u16, u16);
decode_uint!(decode_u32, u32);
decode_uint!(decode_u64, u64);
decode_uint!(decode_u128, u128);

// ── signed integers ───────────────────────────────────────────────────────────

encode_uint!(encode_i8, i8);
encode_uint!(encode_i16, i16);
encode_uint!(encode_i32, i32);
encode_uint!(encode_i64, i64);
encode_uint!(encode_i128, i128);

decode_uint!(decode_i8, i8);
decode_uint!(decode_i16, i16);
decode_uint!(decode_i32, i32);
decode_uint!(decode_i64, i64);
decode_uint!(decode_i128, i128);

// ── floats ────────────────────────────────────────────────────────────────────

#[pyfunction]
fn encode_f32(py: Python<'_>, v: f32) -> PyObject {
    PyBytes::new_bound(py, &v.to_le_bytes()).into()
}

#[pyfunction]
fn decode_f32(buf: &[u8], offset: usize) -> PyResult<(f32, usize)> {
    let bytes: [u8; 4] = buf_slice(buf, offset, 4)?.try_into().unwrap();
    Ok((f32::from_le_bytes(bytes), offset + 4))
}

#[pyfunction]
fn encode_f64(py: Python<'_>, v: f64) -> PyObject {
    PyBytes::new_bound(py, &v.to_le_bytes()).into()
}

#[pyfunction]
fn decode_f64(buf: &[u8], offset: usize) -> PyResult<(f64, usize)> {
    let bytes: [u8; 8] = buf_slice(buf, offset, 8)?.try_into().unwrap();
    Ok((f64::from_le_bytes(bytes), offset + 8))
}

// ── string ────────────────────────────────────────────────────────────────────

/// BSATN string: 4-byte LE length prefix + UTF-8 bytes.
#[pyfunction]
fn encode_string(py: Python<'_>, v: &str) -> PyObject {
    let utf8 = v.as_bytes();
    let len = (utf8.len() as u32).to_le_bytes();
    let mut out = Vec::with_capacity(4 + utf8.len());
    out.extend_from_slice(&len);
    out.extend_from_slice(utf8);
    PyBytes::new_bound(py, &out).into()
}

#[pyfunction]
fn decode_string(buf: &[u8], offset: usize) -> PyResult<(String, usize)> {
    let len_bytes: [u8; 4] = buf_slice(buf, offset, 4)?.try_into().unwrap();
    let len = u32::from_le_bytes(len_bytes) as usize;
    let str_bytes = buf_slice(buf, offset + 4, len)?;
    let s = std::str::from_utf8(str_bytes)
        .map_err(|e| PyValueError::new_err(format!("invalid UTF-8 in string: {e}")))?;
    Ok((s.to_owned(), offset + 4 + len))
}

// ── raw bytes ─────────────────────────────────────────────────────────────────

/// BSATN byte array: 4-byte LE length prefix + raw bytes.
#[pyfunction]
fn encode_bytes_bsatn(py: Python<'_>, v: &[u8]) -> PyObject {
    let len = (v.len() as u32).to_le_bytes();
    let mut out = Vec::with_capacity(4 + v.len());
    out.extend_from_slice(&len);
    out.extend_from_slice(v);
    PyBytes::new_bound(py, &out).into()
}

#[pyfunction]
fn decode_bytes_bsatn<'py>(
    py: Python<'py>,
    buf: &[u8],
    offset: usize,
) -> PyResult<(Bound<'py, PyBytes>, usize)> {
    let len_bytes: [u8; 4] = buf_slice(buf, offset, 4)?.try_into().unwrap();
    let len = u32::from_le_bytes(len_bytes) as usize;
    let data = buf_slice(buf, offset + 4, len)?;
    Ok((PyBytes::new_bound(py, data), offset + 4 + len))
}

// ── module ────────────────────────────────────────────────────────────────────

#[pymodule]
fn bsatn_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(encode_bool, m)?)?;
    m.add_function(wrap_pyfunction!(decode_bool, m)?)?;

    m.add_function(wrap_pyfunction!(encode_u8, m)?)?;
    m.add_function(wrap_pyfunction!(encode_u16, m)?)?;
    m.add_function(wrap_pyfunction!(encode_u32, m)?)?;
    m.add_function(wrap_pyfunction!(encode_u64, m)?)?;
    m.add_function(wrap_pyfunction!(encode_u128, m)?)?;
    m.add_function(wrap_pyfunction!(decode_u8, m)?)?;
    m.add_function(wrap_pyfunction!(decode_u16, m)?)?;
    m.add_function(wrap_pyfunction!(decode_u32, m)?)?;
    m.add_function(wrap_pyfunction!(decode_u64, m)?)?;
    m.add_function(wrap_pyfunction!(decode_u128, m)?)?;

    m.add_function(wrap_pyfunction!(encode_i8, m)?)?;
    m.add_function(wrap_pyfunction!(encode_i16, m)?)?;
    m.add_function(wrap_pyfunction!(encode_i32, m)?)?;
    m.add_function(wrap_pyfunction!(encode_i64, m)?)?;
    m.add_function(wrap_pyfunction!(encode_i128, m)?)?;
    m.add_function(wrap_pyfunction!(decode_i8, m)?)?;
    m.add_function(wrap_pyfunction!(decode_i16, m)?)?;
    m.add_function(wrap_pyfunction!(decode_i32, m)?)?;
    m.add_function(wrap_pyfunction!(decode_i64, m)?)?;
    m.add_function(wrap_pyfunction!(decode_i128, m)?)?;

    m.add_function(wrap_pyfunction!(encode_f32, m)?)?;
    m.add_function(wrap_pyfunction!(decode_f32, m)?)?;
    m.add_function(wrap_pyfunction!(encode_f64, m)?)?;
    m.add_function(wrap_pyfunction!(decode_f64, m)?)?;

    m.add_function(wrap_pyfunction!(encode_string, m)?)?;
    m.add_function(wrap_pyfunction!(decode_string, m)?)?;

    m.add_function(wrap_pyfunction!(encode_bytes_bsatn, m)?)?;
    m.add_function(wrap_pyfunction!(decode_bytes_bsatn, m)?)?;

    Ok(())
}
