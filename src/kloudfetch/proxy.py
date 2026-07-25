"""HTTP HiveServer2 proxy that presents Databricks Cloud Fetch semantics."""

from __future__ import annotations

import base64
import binascii
import http.client
import os
import secrets
import struct
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
from botocore.config import Config

from . import thrift
from .cloudfetch import ManifestStore, inject_result_links, set_has_more_rows
from .operation_state import S3OperationStore

READ_QUERY_PREFIXES = ("SELECT", "WITH", "VALUES", "TABLE")


def setting(name: str, default: str) -> str:
    return os.environ.get(name, default)


def valid_authorization(header: str | None, password: str) -> bool:
    if not password:
        return True
    if not header:
        return False
    scheme, separator, credential = header.partition(" ")
    if not separator:
        return False
    credential = credential.strip()
    if scheme.lower() == "bearer":
        return secrets.compare_digest(credential, password)
    if scheme.lower() != "basic":
        return False
    try:
        decoded = base64.b64decode(credential, validate=True).decode()
        return secrets.compare_digest(decoded, f"token:{password}")
    except (binascii.Error, UnicodeDecodeError):
        return False


def authorization_shape(header: str | None) -> str:
    """Describe an auth header for diagnostics without exposing credentials."""
    if not header:
        return "missing"
    scheme, _, credential = header.partition(" ")
    if scheme.lower() == "basic":
        try:
            decoded = base64.b64decode(credential.strip(), validate=True).decode()
            username, separator, supplied_password = decoded.partition(":")
            return (
                f"basic user={username!r} password_length="
                f"{len(supplied_password) if separator else 0}"
            )
        except (binascii.Error, UnicodeDecodeError):
            return "basic malformed"
    return f"{scheme.lower()} credential_length={len(credential.strip())}"


def add_default_client_protocol(request: bytes) -> tuple[bytes, bool]:
    msg = thrift.message(request)
    if not msg or msg.name != "OpenSession":
        return request, False
    args = thrift.field(request, msg.struct_offset, 1)
    if not args or args.value_type != thrift.T_STRUCT:
        return request, False
    protocol = thrift.field(request, args.value_start, 1)
    if protocol:
        if protocol.value_type != thrift.T_I32:
            return request, False
        value = struct.unpack(
            ">i", request[protocol.value_start : protocol.value_start + 4]
        )[0]
        if value >= 0:
            return request, False
        return (
            request[: protocol.value_start]
            + struct.pack(">i", 10)
            + request[protocol.value_start + 4 :],
            True,
        )
    encoded = bytes([thrift.T_I32]) + struct.pack(">hi", 1, 10)
    return (
        request[: args.value_start] + encoded + request[args.value_start :],
        True,
    )


def restore_databricks_server_protocol(response: bytes, translated: bool) -> bytes:
    if not translated:
        return response
    standard = bytes([thrift.T_I32]) + struct.pack(">hi", 2, 10)
    databricks = bytes([thrift.T_I32]) + struct.pack(">hi", 2, 42249)
    return response.replace(standard, databricks, 1)


def execute_sql(request: bytes) -> tuple[str, thrift.Field] | None:
    msg = thrift.message(request)
    if not msg or msg.name != "ExecuteStatement":
        return None
    args = thrift.field(request, msg.struct_offset, 1)
    if not args or args.value_type != thrift.T_STRUCT:
        return None
    statement = thrift.field(request, args.value_start, 2)
    if not statement or statement.value_type != thrift.T_STRING:
        return None
    return thrift.string_value(request, statement).decode(), statement


def eligible_sql(sql: str) -> bool:
    normalized = sql.lstrip()
    while normalized.startswith("--"):
        _, _, normalized = normalized.partition("\n")
        normalized = normalized.lstrip()
    return normalized.upper().startswith(READ_QUERY_PREFIXES)


def tag_query(request: bytes, query_id: str) -> tuple[bytes, bool]:
    parsed = execute_sql(request)
    if not parsed or not eligible_sql(parsed[0]):
        return request, False
    sql, statement = parsed
    inner = sql.strip().rstrip(";")
    replacement = (
        f"SELECT /*+ KLOUDFETCH('{query_id}') */ * "
        f"FROM ({inner}) AS __kloudfetch_query"
    ).encode()
    return (
        request[: statement.value_start]
        + struct.pack(">I", len(replacement))
        + replacement
        + request[statement.value_end :],
        True,
    )


def operation_handle(request: bytes, methods: set[str]) -> bytes | None:
    msg = thrift.message(request)
    if not msg or msg.name not in methods:
        return None
    args = thrift.field(request, msg.struct_offset, 1)
    if not args or args.value_type != thrift.T_STRUCT:
        return None
    handle = thrift.field(request, args.value_start, 1)
    if not handle or handle.value_type != thrift.T_STRUCT:
        return None
    return request[handle.value_start : handle.value_end]


def fetch_start_row_offset(request: bytes) -> int:
    msg = thrift.message(request)
    if not msg or msg.name != "FetchResults":
        return 0
    args = thrift.field(request, msg.struct_offset, 1)
    if not args or args.value_type != thrift.T_STRUCT:
        return 0
    value = thrift.field(request, args.value_start, 1282)
    if not value or value.value_type != thrift.T_I64:
        return 0
    return struct.unpack(">q", request[value.value_start : value.value_start + 8])[0]


def fetch_orientation(request: bytes) -> int:
    msg = thrift.message(request)
    if not msg or msg.name != "FetchResults":
        return 0
    args = thrift.field(request, msg.struct_offset, 1)
    if not args or args.value_type != thrift.T_STRUCT:
        return 0
    value = thrift.field(request, args.value_start, 2)
    if not value or value.value_type != thrift.T_I32:
        return 0
    return struct.unpack(">i", request[value.value_start : value.value_start + 4])[0]


def normalize_fetch_orientation(request: bytes) -> bytes:
    """Map Databricks FETCH_ABSOLUTE to Spark's supported FETCH_FIRST.

    The absolute row offset is applied to result links by the proxy; Spark's
    empty spooled row iterator only needs to return a successful response.
    """
    msg = thrift.message(request)
    if not msg or msg.name != "FetchResults":
        return request
    args = thrift.field(request, msg.struct_offset, 1)
    if not args or args.value_type != thrift.T_STRUCT:
        return request
    orientation = thrift.field(request, args.value_start, 2)
    if not orientation or orientation.value_type != thrift.T_I32:
        return request
    value = struct.unpack(
        ">i", request[orientation.value_start : orientation.value_start + 4]
    )[0]
    if value != 3:
        return request
    return (
        request[: orientation.value_start]
        + struct.pack(">i", 4)
        + request[orientation.value_start + 4 :]
    )


def operation_handle_from_execute_response(response: bytes) -> bytes | None:
    msg = thrift.message(response)
    if not msg or msg.name != "ExecuteStatement":
        return None
    result = thrift.field(response, msg.struct_offset, 0)
    if not result or result.value_type != thrift.T_STRUCT:
        return None
    handle = thrift.field(response, result.value_start, 2)
    if not handle or handle.value_type != thrift.T_STRUCT:
        return None
    return response[handle.value_start : handle.value_end]


def handle_key(handle: bytes) -> bytes:
    identifier = thrift.field(handle, 0, 1)
    if not identifier or identifier.value_type != thrift.T_STRUCT:
        return handle
    guid = thrift.field(handle, identifier.value_start, 1)
    secret = thrift.field(handle, identifier.value_start, 2)
    if not guid or not secret:
        return handle
    return thrift.string_value(handle, guid) + thrift.string_value(handle, secret)


def replace_operation_handle(request: bytes, replacement: bytes) -> bytes:
    msg = thrift.message(request)
    if not msg:
        return request
    args = thrift.field(request, msg.struct_offset, 1)
    if not args or args.value_type != thrift.T_STRUCT:
        return request
    handle = thrift.field(request, args.value_start, 1)
    if not handle or handle.value_type != thrift.T_STRUCT:
        return request
    return (
        request[: handle.value_start]
        + replacement
        + request[handle.value_end :]
    )


class State:
    def __init__(
        self,
        manifest_store: ManifestStore,
        operation_store: S3OperationStore,
    ) -> None:
        self.manifest_store = manifest_store
        self.operation_store = operation_store

    def register(self, handle: bytes, query_id: str) -> None:
        self.operation_store.create(handle_key(handle), query_id, handle)

    def query_for(self, handle: bytes) -> str | None:
        loaded = self.operation_store.load(handle_key(handle))
        return loaded[0].query_id if loaded else None

    def upstream_handle(self, handle: bytes) -> bytes | None:
        loaded = self.operation_store.load(handle_key(handle))
        return loaded[0].upstream_handle_bytes if loaded else None

    def metadata(self, handle: bytes) -> bytes | None:
        loaded = self.operation_store.load(handle_key(handle))
        return loaded[0].metadata_bytes if loaded else None

    def cache_metadata(self, handle: bytes, response: bytes) -> None:
        encoded = base64.b64encode(response).decode()
        self.operation_store.update(
            handle_key(handle),
            lambda record: setattr(record, "metadata", encoded),
        )

    def fetch_offset(
        self, handle: bytes, orientation: int, requested_offset: int
    ) -> int:
        """Atomically acknowledge progress and resolve the shared cursor."""
        resolved = 0

        def mutate(record) -> None:
            nonlocal resolved
            if orientation in {3, 4}:  # FETCH_ABSOLUTE or FETCH_FIRST
                record.next_offset = requested_offset
            resolved = record.next_offset
            record.acknowledged_offset = max(
                record.acknowledged_offset, resolved
            )

        updated = self.operation_store.update(handle_key(handle), mutate)
        return resolved if updated else requested_offset

    def advance_fetch(self, handle: bytes, next_offset: int) -> None:
        self.operation_store.update(
            handle_key(handle),
            lambda record: setattr(
                record, "next_offset", max(record.next_offset, next_offset)
            ),
        )

    def close(self, handle: bytes) -> None:
        cleanup_at = time.time() + int(
            setting("KLOUDFETCH_CLEANUP_DELAY_SECONDS", "900")
        )
        self.operation_store.update(
            handle_key(handle),
            lambda record: setattr(record, "cleanup_at", cleanup_at),
        )

    def cleanup_due(self, operation_max_age_seconds: int) -> int:
        now = time.time()
        due = self.operation_store.due_for_cleanup(
            now, now - operation_max_age_seconds
        )
        for digest, record in due:
            self.manifest_store.delete_query(record.query_id)
            self.operation_store.delete_digest(digest)
        return len(due)


def build_state() -> State:
    endpoint = setting("KLOUDFETCH_S3_ENDPOINT_URL", "http://rustfs:9000")
    public_endpoint = setting(
        "KLOUDFETCH_S3_PUBLIC_ENDPOINT_URL", "http://localhost:9000"
    )
    common = {
        "aws_access_key_id": setting("AWS_ACCESS_KEY_ID", "kloudfetch"),
        "aws_secret_access_key": setting(
            "AWS_SECRET_ACCESS_KEY", "kloudfetch-secret"
        ),
        "region_name": setting("AWS_REGION", "us-east-1"),
        "config": Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    }
    internal = boto3.client("s3", endpoint_url=endpoint, **common)
    public = boto3.client("s3", endpoint_url=public_endpoint, **common)
    store = ManifestStore(
        internal,
        setting("KLOUDFETCH_S3_BUCKET", "kloudfetch"),
        setting("KLOUDFETCH_S3_PREFIX", "results"),
        public_s3_client=public,
        url_ttl_seconds=int(setting("KLOUDFETCH_URL_TTL_SECONDS", "900")),
        manifest_workers=int(setting("KLOUDFETCH_MANIFEST_WORKERS", "16")),
    )
    operations = S3OperationStore(
        internal,
        setting("KLOUDFETCH_S3_BUCKET", "kloudfetch"),
        setting("KLOUDFETCH_STATE_PREFIX", "kloudfetch-state"),
    )
    return State(store, operations)


STATE: State | None = None


def metadata_request(handle: bytes, sequence_id: int) -> bytes:
    method = b"GetResultSetMetadata"
    return (
        b"\x80\x01\x00\x01"
        + struct.pack(">I", len(method))
        + method
        + struct.pack(">i", sequence_id)
        + b"\x0c\x00\x01"
        + b"\x0c\x00\x01"
        + handle
        + b"\x00"
        + b"\x00"
    )


def add_inline_result_metadata(
    fetch_response: bytes, metadata_response: bytes, result_format: int
) -> bytes:
    fetch_message = thrift.message(fetch_response)
    metadata_message = thrift.message(metadata_response)
    if (
        not fetch_message
        or fetch_message.name != "FetchResults"
        or not metadata_message
        or metadata_message.name != "GetResultSetMetadata"
    ):
        return fetch_response
    fetch_result = thrift.field(fetch_response, fetch_message.struct_offset, 0)
    metadata_result = thrift.field(
        metadata_response, metadata_message.struct_offset, 0
    )
    if (
        not fetch_result
        or fetch_result.value_type != thrift.T_STRUCT
        or not metadata_result
        or metadata_result.value_type != thrift.T_STRUCT
    ):
        return fetch_response
    metadata_struct = metadata_response[
        metadata_result.value_start : metadata_result.value_end
    ]
    # Databricks TSparkRowSetType: ARROW=0, COLUMN=1, ROW=2, URL=3.
    metadata_struct = (
        metadata_struct[:-1]
        + bytes([thrift.T_I32])
        + struct.pack(">hi", 1281, result_format)
        + bytes([thrift.T_STOP])
    )
    inline_field = (
        bytes([thrift.T_STRUCT])
        + struct.pack(">h", 1281)
        + metadata_struct
    )
    insertion = fetch_result.value_end - 1
    return (
        fetch_response[:insertion]
        + inline_field
        + fetch_response[insertion:]
    )


def inject_arrow_schema(metadata_response: bytes, arrow_schema: bytes) -> bytes:
    """Add Databricks' serialized Arrow schema metadata (field 1283)."""
    msg = thrift.message(metadata_response)
    if not msg or msg.name != "GetResultSetMetadata":
        return metadata_response
    result = thrift.field(metadata_response, msg.struct_offset, 0)
    if not result or result.value_type != thrift.T_STRUCT:
        return metadata_response
    if thrift.field(metadata_response, result.value_start, 1283):
        return metadata_response
    encoded = (
        bytes([thrift.T_STRING])
        + struct.pack(">hI", 1283, len(arrow_schema))
        + arrow_schema
    )
    insertion = result.value_end - 1
    return (
        metadata_response[:insertion]
        + encoded
        + metadata_response[insertion:]
    )


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def read_body(self) -> bytes:
        if self.headers.get("Transfer-Encoding", "").lower() != "chunked":
            return self.rfile.read(int(self.headers.get("Content-Length", "0")))
        chunks = []
        while True:
            size = int(self.rfile.readline().split(b";", 1)[0].strip(), 16)
            if size == 0:
                while self.rfile.readline() not in {b"\r\n", b"\n", b""}:
                    pass
                return b"".join(chunks)
            chunks.append(self.rfile.read(size))
            self.rfile.read(2)

    def do_GET(self) -> None:
        self.send_response(200 if self.path == "/health" else 404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self) -> None:
        if self.path != "/cliservice":
            self.send_error(404)
            return
        password = setting("KLOUDFETCH_PASSWORD", "test-token")
        if not valid_authorization(self.headers.get("Authorization"), password):
            self.log_message(
                "authentication rejected: %s",
                authorization_shape(self.headers.get("Authorization")),
            )
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="KloudFetch"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        assert STATE is not None
        original = self.read_body()
        method = thrift.message(original)
        outbound, translated_protocol = add_default_client_protocol(original)
        query_id = uuid.uuid4().hex
        outbound, tagged = tag_query(outbound, query_id)
        outbound = normalize_fetch_orientation(outbound)
        handle = operation_handle(
            original, {"FetchResults", "CancelOperation", "CloseOperation"}
        )
        upstream_operation_handle = None
        if handle:
            upstream_operation_handle = STATE.upstream_handle(handle)
            if upstream_operation_handle:
                outbound = replace_operation_handle(
                    outbound, upstream_operation_handle
                )

        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower()
            not in {
                "authorization",
                "connection",
                "content-length",
                "host",
                "proxy-connection",
                "transfer-encoding",
            }
        }
        upstream_host = setting("KLOUDFETCH_UPSTREAM_HOST", "spark")
        upstream_port = int(setting("KLOUDFETCH_UPSTREAM_PORT", "10001"))
        upstream_username = setting("KLOUDFETCH_UPSTREAM_USERNAME", "token")
        upstream_password = setting("KLOUDFETCH_UPSTREAM_PASSWORD", password)
        headers["Authorization"] = "Basic " + base64.b64encode(
            f"{upstream_username}:{upstream_password}".encode()
        ).decode()
        headers["Host"] = f"{upstream_host}:{upstream_port}"
        connection = http.client.HTTPConnection(upstream_host, upstream_port, timeout=600)
        try:
            connection.request("POST", self.path, outbound, headers)
            upstream = connection.getresponse()
            response = upstream.read()
            response = restore_databricks_server_protocol(
                response, translated_protocol
            )
            if tagged and upstream.status == 200:
                execute_handle = operation_handle_from_execute_response(response)
                if execute_handle:
                    STATE.register(execute_handle, query_id)
            if handle and method and method.name == "FetchResults":
                links = []
                has_more = False
                registered_query = STATE.query_for(handle)
                if registered_query:
                    orientation = fetch_orientation(original)
                    requested_offset = STATE.fetch_offset(
                        handle,
                        orientation,
                        fetch_start_row_offset(original),
                    )
                    links, has_more = STATE.manifest_store.link_page(
                        registered_query,
                        requested_offset,
                        int(setting("KLOUDFETCH_MAX_LINKS_PER_FETCH", "16")),
                    )
                    if links:
                        next_offset = (
                            links[-1].start_row_offset + links[-1].row_count
                        )
                        STATE.advance_fetch(handle, next_offset)
                        self.log_message(
                            "cloud-fetch orientation=%d offset=%d next=%d "
                            "links=%d has_more=%s",
                            orientation,
                            requested_offset,
                            next_offset,
                            len(links),
                            has_more,
                        )
                metadata_response = STATE.metadata(handle)
                if metadata_response is None:
                    metadata_connection = http.client.HTTPConnection(
                        upstream_host, upstream_port, timeout=600
                    )
                    try:
                        metadata_connection.request(
                            "POST",
                            self.path,
                            metadata_request(
                                upstream_operation_handle or handle,
                                method.sequence_id,
                            ),
                            headers,
                        )
                        metadata_http = metadata_connection.getresponse()
                        candidate = metadata_http.read()
                        if metadata_http.status == 200:
                            if registered_query:
                                arrow_schema = (
                                    STATE.manifest_store.arrow_schema(
                                        registered_query
                                    )
                                )
                                if arrow_schema:
                                    candidate = inject_arrow_schema(
                                        candidate, arrow_schema
                                    )
                            STATE.cache_metadata(handle, candidate)
                            metadata_response = candidate
                    finally:
                        metadata_connection.close()
                if metadata_response is not None:
                    response = add_inline_result_metadata(
                        response,
                        metadata_response,
                        3 if links else 1,
                    )
                if links:
                    response = inject_result_links(response, links)
                    response = set_has_more_rows(response, has_more)
            if handle and method and method.name in {
                "CancelOperation",
                "CloseOperation",
            }:
                STATE.close(handle)

            self.send_response(upstream.status)
            for key, value in upstream.getheaders():
                if key.lower() not in {
                    "connection",
                    "content-length",
                    "transfer-encoding",
                }:
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            self.wfile.write(response)
            self.log_message(
                "method=%s status=%d request=%d response=%d tagged=%s links_handle=%s",
                method.name if method else "unknown",
                upstream.status,
                len(original),
                len(response),
                tagged,
                bool(handle),
            )
        finally:
            connection.close()


def main() -> None:
    global STATE
    STATE = build_state()
    cleanup_age = int(setting("KLOUDFETCH_CLEANUP_DELAY_SECONDS", "900"))
    cleanup_interval = int(setting("KLOUDFETCH_CLEANUP_INTERVAL_SECONDS", "60"))
    operation_max_age = int(
        setting("KLOUDFETCH_OPERATION_MAX_AGE_SECONDS", "86400")
    )

    def cleanup_loop() -> None:
        while True:
            time.sleep(cleanup_interval)
            try:
                assert STATE is not None
                removed = STATE.manifest_store.delete_expired(cleanup_age)
                operations = STATE.cleanup_due(operation_max_age)
                if removed or operations:
                    print(
                        "KloudFetch cleanup removed "
                        f"{removed} stale prefixes and {operations} operations",
                        flush=True,
                    )
            except Exception as error:  # noqa: BLE001 - cleanup must stay alive
                print(f"KloudFetch cleanup failed: {error}", flush=True)

    cleanup_thread = threading.Thread(
        target=cleanup_loop, name="kloudfetch-cleanup", daemon=True
    )
    cleanup_thread.start()
    address = setting("KLOUDFETCH_LISTEN_HOST", "0.0.0.0")
    port = int(setting("KLOUDFETCH_LISTEN_PORT", "10000"))
    ThreadingHTTPServer((address, port), ProxyHandler).serve_forever()


if __name__ == "__main__":
    main()
