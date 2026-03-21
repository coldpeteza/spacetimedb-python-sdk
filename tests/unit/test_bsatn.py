"""
Unit tests for spacetimedb_sdk.bsatn

These tests exercise the pure-Python fallback path (no native Rust extension
required) and will also exercise the native extension when it is built.
Every encode/decode pair is tested as a round-trip. Offset advancement is
verified by chaining two decodes from a single concatenated buffer.
"""

import math
import struct
import sys
import os

import pytest

# Allow running without installing the package (PYTHONPATH=src pytest tests/)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from spacetimedb_sdk import bsatn


# ── bool ──────────────────────────────────────────────────────────────────────

def test_encode_bool_true():
    assert bsatn.encode_bool(True) == b'\x01'

def test_encode_bool_false():
    assert bsatn.encode_bool(False) == b'\x00'

def test_decode_bool_true():
    val, off = bsatn.decode_bool(b'\x01', 0)
    assert val is True
    assert off == 1

def test_decode_bool_false():
    val, off = bsatn.decode_bool(b'\x00', 0)
    assert val is False
    assert off == 1

def test_decode_bool_invalid():
    with pytest.raises((ValueError, Exception)):
        bsatn.decode_bool(b'\x02', 0)

def test_bool_roundtrip_with_offset():
    buf = bsatn.encode_bool(False) + bsatn.encode_bool(True)
    v1, off = bsatn.decode_bool(buf, 0)
    v2, off = bsatn.decode_bool(buf, off)
    assert v1 is False
    assert v2 is True
    assert off == 2

# ── unsigned integers ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("v", [0, 1, 127, 255])
def test_u8_roundtrip(v):
    buf = bsatn.encode_u8(v)
    assert len(buf) == 1
    val, off = bsatn.decode_u8(buf, 0)
    assert val == v
    assert off == 1

@pytest.mark.parametrize("v", [0, 1, 256, 65535])
def test_u16_roundtrip(v):
    buf = bsatn.encode_u16(v)
    assert len(buf) == 2
    val, off = bsatn.decode_u16(buf, 0)
    assert val == v
    assert off == 2

@pytest.mark.parametrize("v", [0, 1, 65536, 2**32 - 1])
def test_u32_roundtrip(v):
    buf = bsatn.encode_u32(v)
    assert len(buf) == 4
    val, off = bsatn.decode_u32(buf, 0)
    assert val == v
    assert off == 4

@pytest.mark.parametrize("v", [0, 1, 2**32, 2**64 - 1])
def test_u64_roundtrip(v):
    buf = bsatn.encode_u64(v)
    assert len(buf) == 8
    val, off = bsatn.decode_u64(buf, 0)
    assert val == v
    assert off == 8

@pytest.mark.parametrize("v", [0, 1, 2**64, 2**128 - 1])
def test_u128_roundtrip(v):
    buf = bsatn.encode_u128(v)
    assert len(buf) == 16
    val, off = bsatn.decode_u128(buf, 0)
    assert val == v
    assert off == 16

# ── signed integers ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("v", [-128, -1, 0, 1, 127])
def test_i8_roundtrip(v):
    buf = bsatn.encode_i8(v)
    assert len(buf) == 1
    val, off = bsatn.decode_i8(buf, 0)
    assert val == v
    assert off == 1

@pytest.mark.parametrize("v", [-32768, -1, 0, 1, 32767])
def test_i16_roundtrip(v):
    buf = bsatn.encode_i16(v)
    assert len(buf) == 2
    val, off = bsatn.decode_i16(buf, 0)
    assert val == v
    assert off == 2

@pytest.mark.parametrize("v", [-(2**31), -1, 0, 1, 2**31 - 1])
def test_i32_roundtrip(v):
    buf = bsatn.encode_i32(v)
    assert len(buf) == 4
    val, off = bsatn.decode_i32(buf, 0)
    assert val == v
    assert off == 4

@pytest.mark.parametrize("v", [-(2**63), -1, 0, 1, 2**63 - 1])
def test_i64_roundtrip(v):
    buf = bsatn.encode_i64(v)
    assert len(buf) == 8
    val, off = bsatn.decode_i64(buf, 0)
    assert val == v
    assert off == 8

@pytest.mark.parametrize("v", [-(2**127), -1, 0, 1, 2**127 - 1])
def test_i128_roundtrip(v):
    buf = bsatn.encode_i128(v)
    assert len(buf) == 16
    val, off = bsatn.decode_i128(buf, 0)
    assert val == v
    assert off == 16

# ── little-endian byte order sanity check ─────────────────────────────────────

def test_u32_little_endian():
    # 256 = 0x00000100 → LE bytes: 00 01 00 00
    assert bsatn.encode_u32(256) == b'\x00\x01\x00\x00'

def test_i32_little_endian_negative():
    # -1 in two's complement = 0xFFFFFFFF → LE: ff ff ff ff
    assert bsatn.encode_i32(-1) == b'\xff\xff\xff\xff'

# ── floats ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("v", [0.0, 1.0, -1.0, 3.14])
def test_f32_roundtrip(v):
    buf = bsatn.encode_f32(v)
    assert len(buf) == 4
    val, off = bsatn.decode_f32(buf, 0)
    assert math.isclose(val, v, rel_tol=1e-6)
    assert off == 4

def test_f32_infinity():
    buf = bsatn.encode_f32(math.inf)
    val, _ = bsatn.decode_f32(buf, 0)
    assert math.isinf(val) and val > 0

def test_f32_nan():
    buf = bsatn.encode_f32(math.nan)
    val, _ = bsatn.decode_f32(buf, 0)
    assert math.isnan(val)

@pytest.mark.parametrize("v", [0.0, 1.0, -1.0, 3.141592653589793])
def test_f64_roundtrip(v):
    buf = bsatn.encode_f64(v)
    assert len(buf) == 8
    val, off = bsatn.decode_f64(buf, 0)
    assert math.isclose(val, v, rel_tol=1e-15)
    assert off == 8

def test_f64_infinity():
    buf = bsatn.encode_f64(-math.inf)
    val, _ = bsatn.decode_f64(buf, 0)
    assert math.isinf(val) and val < 0

def test_f64_nan():
    buf = bsatn.encode_f64(math.nan)
    val, _ = bsatn.decode_f64(buf, 0)
    assert math.isnan(val)

# ── string ────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("s", ["", "hello", "SpacetimeDB", "日本語", "emoji 🚀"])
def test_string_roundtrip(s):
    buf = bsatn.encode_string(s)
    utf8_len = len(s.encode('utf-8'))
    assert len(buf) == 4 + utf8_len
    # first 4 bytes must be the LE length
    assert struct.unpack_from('<I', buf, 0)[0] == utf8_len
    val, off = bsatn.decode_string(buf, 0)
    assert val == s
    assert off == 4 + utf8_len

def test_string_offset():
    buf = bsatn.encode_string("ab") + bsatn.encode_string("cd")
    v1, off = bsatn.decode_string(buf, 0)
    v2, off = bsatn.decode_string(buf, off)
    assert v1 == "ab"
    assert v2 == "cd"

# ── raw bytes ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("data", [b"", b"\x00", b"\xde\xad\xbe\xef", bytes(range(16))])
def test_bytes_roundtrip(data):
    buf = bsatn.encode_bytes_bsatn(data)
    assert len(buf) == 4 + len(data)
    assert struct.unpack_from('<I', buf, 0)[0] == len(data)
    val, off = bsatn.decode_bytes_bsatn(buf, 0)
    assert bytes(val) == data
    assert off == 4 + len(data)

# ── composite helpers ─────────────────────────────────────────────────────────

def test_encode_product():
    # Product: concatenation of fields — u32(1) + u32(2)
    result = bsatn.encode_product([bsatn.encode_u32(1), bsatn.encode_u32(2)])
    assert result == bsatn.encode_u32(1) + bsatn.encode_u32(2)
    assert len(result) == 8

def test_encode_sum():
    # Sum: tag byte (u8) + payload
    payload = bsatn.encode_u32(42)
    result = bsatn.encode_sum(3, payload)
    assert result[0:1] == bsatn.encode_u8(3)
    assert result[1:] == payload
    assert len(result) == 5

def test_encode_array_empty():
    result = bsatn.encode_array([])
    assert result == bsatn.encode_u32(0)

def test_encode_array():
    elements = [bsatn.encode_u8(i) for i in range(5)]
    result = bsatn.encode_array(elements)
    length_prefix = bsatn.encode_u32(5)
    assert result == length_prefix + b'\x00\x01\x02\x03\x04'

# ── buffer underflow ──────────────────────────────────────────────────────────

def test_decode_u32_truncated():
    with pytest.raises(Exception):
        bsatn.decode_u32(b'\x01\x02', 0)  # only 2 bytes, need 4

def test_decode_string_truncated():
    # 4-byte length says 100 bytes but buffer is empty after the prefix
    buf = struct.pack('<I', 100)
    with pytest.raises(Exception):
        bsatn.decode_string(buf, 0)

def test_decode_with_nonzero_offset():
    # Encode a u8 followed by a u32; decode the u32 starting at offset 1
    buf = bsatn.encode_u8(0xff) + bsatn.encode_u32(12345)
    val, off = bsatn.decode_u32(buf, 1)
    assert val == 12345
    assert off == 5
