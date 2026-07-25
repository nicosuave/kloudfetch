"""Small, dependency-free helpers for the Thrift binary protocol.

Only the fields needed to proxy HiveServer2 and add Databricks Cloud Fetch
links are implemented. Unknown fields are preserved byte-for-byte.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

T_STOP = 0
T_BOOL = 2
T_BYTE = 3
T_DOUBLE = 4
T_I16 = 6
T_I32 = 8
T_I64 = 10
T_STRING = 11
T_STRUCT = 12
T_MAP = 13
T_SET = 14
T_LIST = 15


@dataclass(frozen=True)
class Message:
    name: str
    sequence_id: int
    struct_offset: int


@dataclass(frozen=True)
class Field:
    value_type: int
    value_start: int
    value_end: int


def message(data: bytes) -> Message | None:
    if len(data) < 12 or data[:2] != b"\x80\x01":
        return None
    name_length = struct.unpack(">I", data[4:8])[0]
    name_end = 8 + name_length
    if name_end + 4 > len(data):
        return None
    return Message(
        data[8:name_end].decode("ascii", errors="replace"),
        struct.unpack(">i", data[name_end : name_end + 4])[0],
        name_end + 4,
    )


def skip_value(data: bytes, offset: int, value_type: int) -> int:
    if value_type in {T_BOOL, T_BYTE}:
        return offset + 1
    if value_type == T_I16:
        return offset + 2
    if value_type == T_I32:
        return offset + 4
    if value_type in {T_DOUBLE, T_I64}:
        return offset + 8
    if value_type == T_STRING:
        length = struct.unpack(">I", data[offset : offset + 4])[0]
        return offset + 4 + length
    if value_type == T_STRUCT:
        while data[offset] != T_STOP:
            nested_type = data[offset]
            offset = skip_value(data, offset + 3, nested_type)
        return offset + 1
    if value_type == T_MAP:
        key_type, item_type = data[offset], data[offset + 1]
        size = struct.unpack(">I", data[offset + 2 : offset + 6])[0]
        offset += 6
        for _ in range(size):
            offset = skip_value(data, offset, key_type)
            offset = skip_value(data, offset, item_type)
        return offset
    if value_type in {T_SET, T_LIST}:
        item_type = data[offset]
        size = struct.unpack(">I", data[offset + 1 : offset + 5])[0]
        offset += 5
        for _ in range(size):
            offset = skip_value(data, offset, item_type)
        return offset
    raise ValueError(f"unsupported Thrift type {value_type}")


def field(data: bytes, struct_offset: int, field_id: int) -> Field | None:
    offset = struct_offset
    while data[offset] != T_STOP:
        value_type = data[offset]
        current_id = struct.unpack(">h", data[offset + 1 : offset + 3])[0]
        value_start = offset + 3
        value_end = skip_value(data, value_start, value_type)
        if current_id == field_id:
            return Field(value_type, value_start, value_end)
        offset = value_end
    return None


def string_value(data: bytes, thrift_field: Field) -> bytes:
    if thrift_field.value_type != T_STRING:
        raise ValueError("field is not a Thrift string/binary")
    length = struct.unpack(
        ">I", data[thrift_field.value_start : thrift_field.value_start + 4]
    )[0]
    start = thrift_field.value_start + 4
    return data[start : start + length]


def encode_string(field_id: int, value: str) -> bytes:
    encoded = value.encode()
    return (
        bytes([T_STRING])
        + struct.pack(">hI", field_id, len(encoded))
        + encoded
    )


def encode_i64(field_id: int, value: int) -> bytes:
    return bytes([T_I64]) + struct.pack(">hq", field_id, value)
