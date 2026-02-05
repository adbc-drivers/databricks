#!/usr/bin/env python3
# Copyright (c) 2025 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Generic Thrift Binary Protocol Decoder.

This module provides utilities to decode Thrift Binary Protocol messages
without requiring specific .thrift IDL files. It works with any Thrift-based
protocol including HiveServer2, Databricks extensions, and custom protocols.

This is particularly useful for:
- Debugging Thrift traffic in proxy tests
- Supporting multiple protocol versions (JDBC vs ADBC)
- Protocol-agnostic logging and inspection
"""

import struct
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple


class ThriftDecoder:
    """
    Generic decoder for Thrift Binary Protocol messages.

    This decoder parses the wire format directly without needing IDL definitions.
    It's forward-compatible with protocol extensions and custom fields.
    """

    # Thrift type constants (from Thrift specification)
    T_STOP = 0
    T_VOID = 1
    T_BOOL = 2
    T_BYTE = 3
    T_I08 = 3
    T_DOUBLE = 4
    T_I16 = 6
    T_I32 = 8
    T_I64 = 10
    T_STRING = 11
    T_UTF7 = 11
    T_STRUCT = 12
    T_MAP = 13
    T_SET = 14
    T_LIST = 15
    T_UTF8 = 16
    T_UTF16 = 17

    TYPE_NAMES = {
        0: "STOP",
        1: "VOID",
        2: "BOOL",
        3: "BYTE",
        4: "DOUBLE",
        6: "I16",
        8: "I32",
        10: "I64",
        11: "STRING",
        12: "STRUCT",
        13: "MAP",
        14: "SET",
        15: "LIST",
        16: "UTF8",
        17: "UTF16",
    }

    # Message types
    MESSAGE_TYPE_CALL = 1
    MESSAGE_TYPE_REPLY = 2
    MESSAGE_TYPE_EXCEPTION = 3
    MESSAGE_TYPE_ONEWAY = 4

    MESSAGE_TYPE_NAMES = {
        1: "CALL",
        2: "REPLY",
        3: "EXCEPTION",
        4: "ONEWAY",
    }

    # Common HiveServer2/Databricks method names (for better logging)
    KNOWN_METHODS = {
        "OpenSession",
        "CloseSession",
        "ExecuteStatement",
        "GetOperationStatus",
        "FetchResults",
        "CloseOperation",
        "CancelOperation",
        "GetResultSetMetadata",
        "GetSchemas",
        "GetTables",
        "GetColumns",
        "GetCatalogs",
        "GetTableTypes",
        "GetTypeInfo",
    }

    def __init__(self, data: bytes):
        """Initialize decoder with Thrift message bytes."""
        self.stream = BytesIO(data)
        self.pos = 0
        self.data_len = len(data)

    def read_byte(self) -> int:
        """Read a single byte."""
        b = self.stream.read(1)
        if len(b) == 0:
            raise EOFError(f"Unexpected end of stream at position {self.pos}")
        self.pos += 1
        return struct.unpack("!b", b)[0]

    def read_i16(self) -> int:
        """Read a 16-bit signed integer."""
        data = self.stream.read(2)
        if len(data) < 2:
            raise EOFError(f"Unexpected end of stream at position {self.pos}")
        self.pos += 2
        return struct.unpack("!h", data)[0]

    def read_i32(self) -> int:
        """Read a 32-bit signed integer."""
        data = self.stream.read(4)
        if len(data) < 4:
            raise EOFError(f"Unexpected end of stream at position {self.pos}")
        self.pos += 4
        return struct.unpack("!i", data)[0]

    def read_i64(self) -> int:
        """Read a 64-bit signed integer."""
        data = self.stream.read(8)
        if len(data) < 8:
            raise EOFError(f"Unexpected end of stream at position {self.pos}")
        self.pos += 8
        return struct.unpack("!q", data)[0]

    def read_double(self) -> float:
        """Read a double-precision float."""
        data = self.stream.read(8)
        if len(data) < 8:
            raise EOFError(f"Unexpected end of stream at position {self.pos}")
        self.pos += 8
        return struct.unpack("!d", data)[0]

    def read_string(self) -> str:
        """Read a length-prefixed string."""
        length = self.read_i32()
        if length < 0:
            raise ValueError(f"Invalid string length: {length}")
        if length > 10 * 1024 * 1024:  # 10MB sanity check
            raise ValueError(f"String length too large: {length}")

        data = self.stream.read(length)
        if len(data) < length:
            raise EOFError(
                f"Unexpected end of stream while reading string at position {self.pos}"
            )
        self.pos += length

        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            # Return hex representation if not valid UTF-8
            preview = data[:50].hex()
            if len(data) > 50:
                preview += "..."
            return f"<binary:{preview}>"

    def read_message_begin(self) -> Tuple[str, int, int]:
        """
        Read Thrift message header.

        Returns:
            Tuple of (method_name, message_type, sequence_id)
        """
        size = self.read_i32()

        if size < 0:
            # Strict mode (version prefix)
            version = size & 0xFFFF0000
            if version != 0x80010000:  # VERSION_1
                raise ValueError(f"Unsupported Thrift version: {hex(version)}")
            message_type = size & 0x000000FF
            method_name = self.read_string()
        else:
            # Non-strict mode (old style)
            method_name_bytes = self.stream.read(size)
            if len(method_name_bytes) < size:
                raise EOFError("Unexpected end of stream while reading method name")
            self.pos += size
            method_name = method_name_bytes.decode("utf-8")
            message_type = self.read_byte()

        sequence_id = self.read_i32()
        return method_name, message_type, sequence_id

    def skip_field(self, field_type: int, max_depth: int = 32) -> None:
        """Skip a field of given type without parsing its value."""
        if max_depth <= 0:
            raise ValueError("Maximum recursion depth exceeded while skipping field")

        if field_type == self.T_BOOL or field_type == self.T_BYTE:
            self.read_byte()
        elif field_type == self.T_I16:
            self.read_i16()
        elif field_type == self.T_I32:
            self.read_i32()
        elif field_type == self.T_I64:
            self.read_i64()
        elif field_type == self.T_DOUBLE:
            self.read_double()
        elif field_type == self.T_STRING:
            self.read_string()
        elif field_type == self.T_STRUCT:
            self._skip_struct(max_depth - 1)
        elif field_type == self.T_MAP:
            key_type = self.read_byte()
            val_type = self.read_byte()
            size = self.read_i32()
            for _ in range(size):
                self.skip_field(key_type, max_depth - 1)
                self.skip_field(val_type, max_depth - 1)
        elif field_type in (self.T_SET, self.T_LIST):
            elem_type = self.read_byte()
            size = self.read_i32()
            for _ in range(size):
                self.skip_field(elem_type, max_depth - 1)

    def _skip_struct(self, max_depth: int) -> None:
        """Skip an entire struct."""
        while True:
            field_type = self.read_byte()
            if field_type == self.T_STOP:
                break
            self.read_i16()  # field_id
            self.skip_field(field_type, max_depth)

    def read_field_value(self, field_type: int, max_depth: int = 32) -> Any:
        """Read and return a field value based on its type."""
        if max_depth <= 0:
            return "<max_depth_exceeded>"

        if field_type == self.T_BOOL:
            return bool(self.read_byte())
        elif field_type == self.T_BYTE:
            return self.read_byte()
        elif field_type == self.T_I16:
            return self.read_i16()
        elif field_type == self.T_I32:
            return self.read_i32()
        elif field_type == self.T_I64:
            return self.read_i64()
        elif field_type == self.T_DOUBLE:
            return self.read_double()
        elif field_type == self.T_STRING:
            return self.read_string()
        elif field_type == self.T_STRUCT:
            return self.read_struct(max_depth - 1)
        elif field_type == self.T_MAP:
            return self._read_map(max_depth - 1)
        elif field_type == self.T_SET:
            return self._read_set(max_depth - 1)
        elif field_type == self.T_LIST:
            return self._read_list(max_depth - 1)
        else:
            return f"<unknown_type:{field_type}>"

    def _read_map(self, max_depth: int) -> Dict[Any, Any]:
        """Read a Thrift map."""
        key_type = self.read_byte()
        val_type = self.read_byte()
        size = self.read_i32()

        if size > 10000:  # Sanity check
            return {"<large_map>": f"{size} entries"}

        result = {}
        for _ in range(size):
            key = self.read_field_value(key_type, max_depth)
            value = self.read_field_value(val_type, max_depth)
            result[key] = value
        return result

    def _read_set(self, max_depth: int) -> List[Any]:
        """Read a Thrift set (returned as list)."""
        elem_type = self.read_byte()
        size = self.read_i32()

        if size > 10000:  # Sanity check
            return [f"<large_set: {size} entries>"]

        result = []
        for _ in range(size):
            result.append(self.read_field_value(elem_type, max_depth))
        return result

    def _read_list(self, max_depth: int) -> List[Any]:
        """Read a Thrift list."""
        elem_type = self.read_byte()
        size = self.read_i32()

        if size > 10000:  # Sanity check
            return [f"<large_list: {size} entries>"]

        result = []
        for _ in range(size):
            result.append(self.read_field_value(elem_type, max_depth))
        return result

    def read_struct(self, max_depth: int = 32) -> Dict[str, Any]:
        """
        Read a Thrift struct and return field map.

        Returns:
            Dictionary mapping field_id to {type, value}
        """
        if max_depth <= 0:
            return {"error": "max_depth_exceeded"}

        fields = {}
        while True:
            try:
                field_type = self.read_byte()
                if field_type == self.T_STOP:
                    break

                field_id = self.read_i16()
                type_name = self.TYPE_NAMES.get(field_type, f"type_{field_type}")

                try:
                    value = self.read_field_value(field_type, max_depth)
                    fields[f"field_{field_id}"] = {"type": type_name, "value": value}
                except Exception as e:
                    fields[f"field_{field_id}"] = {"type": type_name, "error": str(e)}
                    # Try to skip and continue
                    try:
                        self.skip_field(field_type, max_depth)
                    except Exception:
                        break

            except EOFError:
                break
            except Exception as e:
                fields["_parse_error"] = str(e)
                break

        return fields

    def decode_message(self) -> Optional[Dict[str, Any]]:
        """
        Decode a complete Thrift message.

        Returns:
            Dictionary with message information and decoded fields
        """
        try:
            method_name, message_type, sequence_id = self.read_message_begin()

            message_type_str = self.MESSAGE_TYPE_NAMES.get(
                message_type, f"UNKNOWN({message_type})"
            )

            # Read the struct (request/response body)
            struct_fields = self.read_struct()

            result = {
                "method": method_name,
                "message_type": message_type_str,
                "sequence_id": sequence_id,
                "fields": struct_fields,
                "bytes_decoded": self.pos,
                "total_bytes": self.data_len,
            }

            # Add metadata
            if method_name in self.KNOWN_METHODS:
                result["protocol"] = "HiveServer2/Databricks"

            return result

        except Exception as e:
            return {
                "error": str(e),
                "error_type": type(e).__name__,
                "bytes_decoded": self.pos,
                "total_bytes": self.data_len,
            }


def decode_thrift_message(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Decode a Thrift Binary Protocol message.

    Args:
        data: Raw Thrift message bytes

    Returns:
        Dictionary with decoded message info, or None if data is invalid
    """
    if not data or len(data) < 4:
        return None

    try:
        decoder = ThriftDecoder(data)
        return decoder.decode_message()
    except Exception as e:
        return {"error": str(e), "error_type": type(e).__name__}


def format_thrift_message(
    decoded: Dict[str, Any], max_field_length: int = 200, indent: int = 0
) -> str:
    """
    Format a decoded Thrift message for human-readable logging.

    Args:
        decoded: Decoded message dictionary from decode_thrift_message()
        max_field_length: Maximum length for field values in output
        indent: Indentation level for nested structures

    Returns:
        Formatted string representation
    """
    if not decoded:
        return "<empty message>"

    if "error" in decoded:
        return f"<decode error: {decoded['error']}>"

    prefix = "  " * indent
    lines = []

    # Header info
    lines.append(f"{prefix}Method: {decoded.get('method', 'unknown')}")
    lines.append(f"{prefix}Type: {decoded.get('message_type', 'unknown')}")
    lines.append(
        f"{prefix}Bytes: {decoded.get('bytes_decoded', 0)}/{decoded.get('total_bytes', 0)}"
    )

    if "protocol" in decoded:
        lines.append(f"{prefix}Protocol: {decoded['protocol']}")

    # Fields
    fields = decoded.get("fields", {})
    if fields:
        lines.append(f"{prefix}Fields ({len(fields)}):")
        for field_name, field_data in sorted(fields.items()):
            if isinstance(field_data, dict) and "type" in field_data:
                field_type = field_data["type"]
                field_value = field_data.get(
                    "value", field_data.get("error", "<no value>")
                )

                value_str = str(field_value)
                if len(value_str) > max_field_length:
                    value_str = value_str[:max_field_length] + "..."

                lines.append(f"{prefix}  {field_name} ({field_type}): {value_str}")
            else:
                value_str = str(field_data)
                if len(value_str) > max_field_length:
                    value_str = value_str[:max_field_length] + "..."
                lines.append(f"{prefix}  {field_name}: {value_str}")

    return "\n".join(lines)
