"""
BSATN (Binary SpacetimeDB Algebraic Type Notation) encoder/decoder.

Tries to import the compiled Rust extension (bsatn_native) for best
performance.  Falls back to a pure-Python implementation using the
stdlib struct module if the native extension is not available (e.g.
running from source without building the Rust crate).

Public API
----------
All encode functions accept a Python value and return bytes.
All decode functions accept (buf: bytes, offset: int) and return
(value, new_offset) so callers can advance through a buffer sequentially.

Composite types (ProductValue / SumValue) are assembled by callers:
  - Product: concatenate the encoded fields in order.
  - Sum: prepend a single tag byte (encode_u8) then append the payload.
  - Array: encode_u32(len) + repeated element encodings.
"""

import struct as _struct

# ── attempt native import ─────────────────────────────────────────────────────

try:
    import bsatn_native as _native
    _USE_NATIVE = hasattr(_native, 'encode_u8')
except ImportError:
    _native = None
    _USE_NATIVE = False

# ── bool ──────────────────────────────────────────────────────────────────────

def encode_bool(v: bool) -> bytes:
    if _USE_NATIVE:
        return _native.encode_bool(v)
    return b'\x01' if v else b'\x00'

def decode_bool(buf: bytes, offset: int) -> tuple:
    if _USE_NATIVE:
        return _native.decode_bool(buf, offset)
    b = buf[offset]
    if b == 0:
        return (False, offset + 1)
    if b == 1:
        return (True, offset + 1)
    raise ValueError(f"invalid bool byte {b:#04x} at offset {offset}")

# ── unsigned integers ─────────────────────────────────────────────────────────

def encode_u8(v: int) -> bytes:
    return _native.encode_u8(v) if _USE_NATIVE else _struct.pack('<B', v)

def decode_u8(buf: bytes, offset: int) -> tuple:
    return _native.decode_u8(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<B', buf, offset)[0], offset + 1)

def encode_u16(v: int) -> bytes:
    return _native.encode_u16(v) if _USE_NATIVE else _struct.pack('<H', v)

def decode_u16(buf: bytes, offset: int) -> tuple:
    return _native.decode_u16(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<H', buf, offset)[0], offset + 2)

def encode_u32(v: int) -> bytes:
    return _native.encode_u32(v) if _USE_NATIVE else _struct.pack('<I', v)

def decode_u32(buf: bytes, offset: int) -> tuple:
    return _native.decode_u32(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<I', buf, offset)[0], offset + 4)

def encode_u64(v: int) -> bytes:
    return _native.encode_u64(v) if _USE_NATIVE else _struct.pack('<Q', v)

def decode_u64(buf: bytes, offset: int) -> tuple:
    return _native.decode_u64(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<Q', buf, offset)[0], offset + 8)

def encode_u128(v: int) -> bytes:
    return _native.encode_u128(v) if _USE_NATIVE else v.to_bytes(16, 'little', signed=False)

def decode_u128(buf: bytes, offset: int) -> tuple:
    if _USE_NATIVE:
        return _native.decode_u128(buf, offset)
    return (int.from_bytes(buf[offset:offset + 16], 'little', signed=False), offset + 16)

# ── signed integers ───────────────────────────────────────────────────────────

def encode_i8(v: int) -> bytes:
    return _native.encode_i8(v) if _USE_NATIVE else _struct.pack('<b', v)

def decode_i8(buf: bytes, offset: int) -> tuple:
    return _native.decode_i8(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<b', buf, offset)[0], offset + 1)

def encode_i16(v: int) -> bytes:
    return _native.encode_i16(v) if _USE_NATIVE else _struct.pack('<h', v)

def decode_i16(buf: bytes, offset: int) -> tuple:
    return _native.decode_i16(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<h', buf, offset)[0], offset + 2)

def encode_i32(v: int) -> bytes:
    return _native.encode_i32(v) if _USE_NATIVE else _struct.pack('<i', v)

def decode_i32(buf: bytes, offset: int) -> tuple:
    return _native.decode_i32(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<i', buf, offset)[0], offset + 4)

def encode_i64(v: int) -> bytes:
    return _native.encode_i64(v) if _USE_NATIVE else _struct.pack('<q', v)

def decode_i64(buf: bytes, offset: int) -> tuple:
    return _native.decode_i64(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<q', buf, offset)[0], offset + 8)

def encode_i128(v: int) -> bytes:
    return _native.encode_i128(v) if _USE_NATIVE else v.to_bytes(16, 'little', signed=True)

def decode_i128(buf: bytes, offset: int) -> tuple:
    if _USE_NATIVE:
        return _native.decode_i128(buf, offset)
    return (int.from_bytes(buf[offset:offset + 16], 'little', signed=True), offset + 16)

# ── floats ────────────────────────────────────────────────────────────────────

def encode_f32(v: float) -> bytes:
    return _native.encode_f32(v) if _USE_NATIVE else _struct.pack('<f', v)

def decode_f32(buf: bytes, offset: int) -> tuple:
    return _native.decode_f32(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<f', buf, offset)[0], offset + 4)

def encode_f64(v: float) -> bytes:
    return _native.encode_f64(v) if _USE_NATIVE else _struct.pack('<d', v)

def decode_f64(buf: bytes, offset: int) -> tuple:
    return _native.decode_f64(buf, offset) if _USE_NATIVE else (_struct.unpack_from('<d', buf, offset)[0], offset + 8)

# ── string ────────────────────────────────────────────────────────────────────

def encode_string(v: str) -> bytes:
    if _USE_NATIVE:
        return _native.encode_string(v)
    utf8 = v.encode('utf-8')
    return _struct.pack('<I', len(utf8)) + utf8

def decode_string(buf: bytes, offset: int) -> tuple:
    if _USE_NATIVE:
        return _native.decode_string(buf, offset)
    (length,) = _struct.unpack_from('<I', buf, offset)
    start = offset + 4
    if len(buf) < start + length:
        raise ValueError(
            f"buffer too short for string: need {length} bytes at offset {start}, have {len(buf) - start}"
        )
    return (buf[start:start + length].decode('utf-8'), start + length)

# ── raw bytes ─────────────────────────────────────────────────────────────────

def encode_bytes_bsatn(v: bytes) -> bytes:
    if _USE_NATIVE:
        return _native.encode_bytes_bsatn(v)
    return _struct.pack('<I', len(v)) + v

def decode_bytes_bsatn(buf: bytes, offset: int) -> tuple:
    if _USE_NATIVE:
        return _native.decode_bytes_bsatn(buf, offset)
    (length,) = _struct.unpack_from('<I', buf, offset)
    start = offset + 4
    return (bytes(buf[start:start + length]), start + length)

# ── composite helpers ─────────────────────────────────────────────────────────

def encode_array(element_bytes: list) -> bytes:
    """Encode an already-encoded list of elements as a BSATN array.

    Callers encode each element first then pass the resulting bytes here:
        encode_array([encode_u32(x) for x in values])
    """
    return encode_u32(len(element_bytes)) + b''.join(element_bytes)

def encode_sum(tag: int, payload: bytes) -> bytes:
    """Encode a SumValue (enum variant): tag byte + encoded payload."""
    return encode_u8(tag) + payload

def encode_product(field_bytes: list) -> bytes:
    """Encode a ProductValue (struct): concatenation of encoded fields."""
    return b''.join(field_bytes)
