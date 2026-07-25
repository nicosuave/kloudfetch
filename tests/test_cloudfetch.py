import struct

from kloudfetch import thrift
from kloudfetch.cloudfetch import (
    ResultLink,
    inject_result_links,
    set_has_more_rows,
)
from kloudfetch.proxy import (
    add_default_client_protocol,
    eligible_sql,
    fetch_orientation,
    handle_key,
    inject_arrow_schema,
    normalize_fetch_orientation,
    restore_databricks_server_protocol,
    tag_query,
)


def message_header(name: str, message_type: int = 2) -> bytes:
    encoded = name.encode()
    return (
        b"\x80\x01"
        + struct.pack(">hI", message_type, len(encoded))
        + encoded
        + struct.pack(">i", 1)
    )


def execute_statement(sql: str) -> bytes:
    encoded = sql.encode()
    request = (
        b"\x0c\x00\x01"
        + b"\x00"
        + b"\x0b\x00\x02"
        + struct.pack(">I", len(encoded))
        + encoded
        + b"\x00"
    )
    return (
        message_header("ExecuteStatement", 1)
        + b"\x0c\x00\x01"
        + request
        + b"\x00"
    )


def fetch_response() -> bytes:
    status = b"\x0c\x00\x01\x08\x00\x01\x00\x00\x00\x00\x00"
    rowset = (
        b"\x0a\x00\x01"
        + struct.pack(">q", 0)
        + b"\x0f\x00\x02\x0c"
        + struct.pack(">I", 0)
        + b"\x00"
    )
    response = (
        b"\x0c\x00\x03"
        + rowset
        + b"\x02\x00\x02\x00"
        + b"\x00"
    )
    return (
        message_header("FetchResults")
        + b"\x0c\x00\x00"
        + status
        + response
        + b"\x00"
    )


def test_query_tagging_wraps_read_queries_and_preserves_commands():
    tagged, changed = tag_query(execute_statement("WITH t AS (SELECT 1) SELECT * FROM t"), "a" * 32)
    assert changed
    assert b"KLOUDFETCH('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')" in tagged
    assert b"WITH t AS (SELECT 1) SELECT * FROM t" in tagged

    command = execute_statement("CREATE TABLE x (id INT)")
    assert tag_query(command, "b" * 32) == (command, False)


def test_query_eligibility_is_conservative():
    assert eligible_sql(" SELECT 1")
    assert eligible_sql("-- comment\nVALUES (1)")
    assert not eligible_sql("INSERT INTO t VALUES (1)")
    assert not eligible_sql("SHOW TABLES")


def test_cloud_fetch_links_use_databricks_rowset_field_1282():
    response = fetch_response()
    link = ResultLink(
        url="http://rustfs.example/bucket/chunk.arrow?signature=abc",
        expiry_time=1_900_000_000,
        start_row_offset=12,
        row_count=34,
        byte_count=567,
    )
    encoded = inject_result_links(response, [link])
    msg = thrift.message(encoded)
    outer = thrift.field(encoded, msg.struct_offset, 0)
    rowset = thrift.field(encoded, outer.value_start, 3)
    result_links = thrift.field(encoded, rowset.value_start, 1282)

    assert result_links is not None
    assert result_links.value_type == thrift.T_LIST
    assert b"chunk.arrow?signature=abc" in encoded
    assert struct.pack(">q", 34) in encoded
    assert inject_result_links(encoded, [link]) == encoded


def test_cloud_fetch_pagination_sets_standard_has_more_rows():
    response = set_has_more_rows(fetch_response(), True)
    msg = thrift.message(response)
    outer = thrift.field(response, msg.struct_offset, 0)
    has_more = thrift.field(response, outer.value_start, 2)

    assert has_more is not None
    assert response[has_more.value_start : has_more.value_end] == b"\x01"
    cleared = set_has_more_rows(response, False)
    msg = thrift.message(cleared)
    outer = thrift.field(cleared, msg.struct_offset, 0)
    has_more = thrift.field(cleared, outer.value_start, 2)
    assert cleared[has_more.value_start : has_more.value_end] == b"\x00"


def test_databricks_protocol_value_is_restored():
    method = b"OpenSession"
    request = (
        b"\x80\x01\x00\x01"
        + struct.pack(">I", len(method))
        + method
        + struct.pack(">i", 1)
        + b"\x0c\x00\x01"
        + b"\x08\x00\x01"
        + struct.pack(">i", -1)
        + b"\x00"
        + b"\x00"
    )
    translated, changed = add_default_client_protocol(request)
    assert changed
    assert struct.pack(">i", 10) in translated

    response = b"prefix" + b"\x08\x00\x02" + struct.pack(">i", 10) + b"suffix"
    restored = restore_databricks_server_protocol(response, True)
    assert struct.pack(">i", 42249) in restored


def test_operation_handle_key_ignores_driver_reconstructed_operation_type():
    identifier = (
        b"\x0b\x00\x01\x00\x00\x00\x10" + b"g" * 16
        + b"\x0b\x00\x02\x00\x00\x00\x10" + b"s" * 16
        + b"\x00"
    )
    execute_handle = (
        b"\x0c\x00\x01"
        + identifier
        + b"\x08\x00\x02\x00\x00\x00\x05"
        + b"\x02\x00\x03\x01"
        + b"\x00"
    )
    reconstructed = execute_handle.replace(
        b"\x08\x00\x02\x00\x00\x00\x05",
        b"\x08\x00\x02\x00\x00\x00\x00",
    )

    assert handle_key(execute_handle) == handle_key(reconstructed)
    assert handle_key(execute_handle) == b"g" * 16 + b"s" * 16


def test_fetch_absolute_is_normalized_for_stock_spark():
    handle = b"\x00"
    request_struct = (
        b"\x0c\x00\x01"
        + handle
        + b"\x08\x00\x02\x00\x00\x00\x03"
        + b"\x00"
    )
    request = (
        message_header("FetchResults", 1)
        + b"\x0c\x00\x01"
        + request_struct
        + b"\x00"
    )

    normalized = normalize_fetch_orientation(request)

    assert fetch_orientation(request) == 3
    assert b"\x08\x00\x02\x00\x00\x00\x04" in normalized


def test_databricks_arrow_schema_is_injected_into_metadata():
    response = (
        message_header("GetResultSetMetadata")
        + b"\x0c\x00\x00"
        + b"\x0c\x00\x01\x08\x00\x01\x00\x00\x00\x00\x00"
        + b"\x00"
    )
    arrow_schema = b"serialized-arrow-schema"

    enriched = inject_arrow_schema(response, arrow_schema)
    msg = thrift.message(enriched)
    result = thrift.field(enriched, msg.struct_offset, 0)
    field = thrift.field(enriched, result.value_start, 1283)

    assert field is not None
    assert thrift.string_value(enriched, field) == arrow_schema
    assert inject_arrow_schema(enriched, arrow_schema) == enriched
