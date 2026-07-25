"""Cloud Fetch manifest discovery and Thrift response encoding."""

from __future__ import annotations

import base64
import json
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from . import thrift

RESULT_LINKS_FIELD_ID = 1282


@dataclass(frozen=True)
class Segment:
    key: str
    row_count: int
    byte_count: int
    partition_id: int
    batch_id: int
    arrow_schema: bytes | None = None


@dataclass(frozen=True)
class ResultLink:
    url: str
    expiry_time: int
    start_row_offset: int
    row_count: int
    byte_count: int


class ManifestStore:
    def __init__(
        self,
        s3_client: Any,
        bucket: str,
        prefix: str,
        public_s3_client: Any | None = None,
        url_ttl_seconds: int = 900,
        manifest_workers: int = 16,
    ) -> None:
        self.s3 = s3_client
        self.public_s3 = public_s3_client or s3_client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.url_ttl_seconds = url_ttl_seconds
        self.manifest_workers = max(1, manifest_workers)
        self._segments_by_query: dict[str, tuple[Segment, ...]] = {}
        self._cache_lock = threading.Lock()

    def _read_segment(self, key: str) -> Segment:
        body = self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        value = json.loads(body)
        return Segment(
            key=str(value["key"]),
            row_count=int(value["rows"]),
            byte_count=int(value["bytes"]),
            partition_id=int(value["partition"]),
            batch_id=int(value["batch"]),
            arrow_schema=(
                base64.b64decode(value["arrow_schema"])
                if value.get("arrow_schema")
                else None
            ),
        )

    def segments(self, query_id: str) -> tuple[Segment, ...]:
        with self._cache_lock:
            cached = self._segments_by_query.get(query_id)
        if cached is not None:
            return cached

        prefix = f"{self.prefix}/{query_id}/"
        paginator = self.s3.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.endswith(".json"):
                    keys.append(key)
        with ThreadPoolExecutor(max_workers=self.manifest_workers) as executor:
            sidecars = list(executor.map(self._read_segment, keys))
        sidecars.sort(key=lambda part: (part.partition_id, part.batch_id))
        result = tuple(sidecars)
        with self._cache_lock:
            # A completed query manifest is immutable. If concurrent first fetches
            # raced, either complete manifest is equivalent.
            self._segments_by_query[query_id] = result
        return result

    def arrow_schema(self, query_id: str) -> bytes | None:
        return next(
            (
                segment.arrow_schema
                for segment in self.segments(query_id)
                if segment.arrow_schema
            ),
            None,
        )

    def _sign_link(
        self, segment: Segment, start_row_offset: int, expiry: int
    ) -> ResultLink:
        url = self.public_s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": segment.key},
            ExpiresIn=self.url_ttl_seconds,
        )
        return ResultLink(
            url=url,
            expiry_time=expiry,
            start_row_offset=start_row_offset,
            row_count=segment.row_count,
            byte_count=segment.byte_count,
        )

    def link_page(
        self, query_id: str, start_row_offset: int = 0, max_links: int = 32
    ) -> tuple[list[ResultLink], bool]:
        segments = self.segments(query_id)
        indexed: list[tuple[Segment, int]] = []
        offset = 0
        for segment in segments:
            if offset + segment.row_count > start_row_offset:
                indexed.append((segment, offset))
            offset += segment.row_count
        page = indexed[:max(1, max_links)]
        has_more = len(indexed) > len(page)
        # Databricks TSparkArrowResultLink.expiryTime is Unix epoch
        # milliseconds (the OSS JDBC driver uses Instant.ofEpochMilli).
        expiry = int(time.time() * 1000) + self.url_ttl_seconds * 1000
        with ThreadPoolExecutor(
            max_workers=min(self.manifest_workers, max(1, len(page)))
        ) as executor:
            links = list(
                executor.map(
                    lambda item: self._sign_link(item[0], item[1], expiry),
                    page,
                )
            )
        return links, has_more

    def links(self, query_id: str) -> list[ResultLink]:
        """Compatibility helper returning an entire manifest."""
        links, _ = self.link_page(query_id, max_links=2**31 - 1)
        return links

    def delete_query(self, query_id: str) -> None:
        with self._cache_lock:
            self._segments_by_query.pop(query_id, None)
        prefix = f"{self.prefix}/{query_id}/"
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                self.s3.delete_objects(
                    Bucket=self.bucket, Delete={"Objects": objects, "Quiet": True}
                )

    def delete_expired(self, max_age_seconds: int) -> int:
        """Delete stale query prefixes, including leftovers from failed jobs."""
        root = f"{self.prefix}/"
        cutoff = datetime.now(UTC).timestamp() - max_age_seconds
        newest_by_query: dict[str, float] = {}
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=root):
            for item in page.get("Contents", []):
                suffix = item["Key"][len(root) :]
                query_id, separator, _ = suffix.partition("/")
                if not separator:
                    continue
                modified = item["LastModified"].timestamp()
                newest_by_query[query_id] = max(
                    newest_by_query.get(query_id, 0), modified
                )
        expired = [
            query_id
            for query_id, newest in newest_by_query.items()
            if newest < cutoff
        ]
        for query_id in expired:
            self.delete_query(query_id)
        return len(expired)


def encode_result_link(link: ResultLink) -> bytes:
    return (
        thrift.encode_string(1, link.url)
        + thrift.encode_i64(2, link.expiry_time)
        + thrift.encode_i64(3, link.start_row_offset)
        + thrift.encode_i64(4, link.row_count)
        + thrift.encode_i64(5, link.byte_count)
        + bytes([thrift.T_STOP])
    )


def inject_result_links(fetch_response: bytes, links: list[ResultLink]) -> bytes:
    """Append Databricks TRowSet.resultLinks (field 1282) to FetchResults."""
    msg = thrift.message(fetch_response)
    if not msg or msg.name != "FetchResults":
        return fetch_response
    response_struct = thrift.field(fetch_response, msg.struct_offset, 0)
    if not response_struct or response_struct.value_type != thrift.T_STRUCT:
        return fetch_response
    rowset = thrift.field(fetch_response, response_struct.value_start, 3)
    if not rowset or rowset.value_type != thrift.T_STRUCT:
        return fetch_response
    if thrift.field(fetch_response, rowset.value_start, RESULT_LINKS_FIELD_ID):
        return fetch_response
    encoded_links = (
        bytes([thrift.T_LIST])
        + struct.pack(">h", RESULT_LINKS_FIELD_ID)
        + bytes([thrift.T_STRUCT])
        + struct.pack(">I", len(links))
        + b"".join(encode_result_link(link) for link in links)
    )
    insertion = rowset.value_end - 1
    return fetch_response[:insertion] + encoded_links + fetch_response[insertion:]


def set_has_more_rows(fetch_response: bytes, has_more: bool) -> bytes:
    """Set the standard TFetchResultsResp.hasMoreRows field."""
    msg = thrift.message(fetch_response)
    if not msg or msg.name != "FetchResults":
        return fetch_response
    response_struct = thrift.field(fetch_response, msg.struct_offset, 0)
    if not response_struct or response_struct.value_type != thrift.T_STRUCT:
        return fetch_response
    existing = thrift.field(fetch_response, response_struct.value_start, 2)
    encoded_value = b"\x01" if has_more else b"\x00"
    if existing and existing.value_type == thrift.T_BOOL:
        return (
            fetch_response[: existing.value_start]
            + encoded_value
            + fetch_response[existing.value_end :]
        )
    encoded_field = bytes([thrift.T_BOOL]) + struct.pack(">h", 2) + encoded_value
    insertion = response_struct.value_end - 1
    return fetch_response[:insertion] + encoded_field + fetch_response[insertion:]
