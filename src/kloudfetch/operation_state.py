"""Durable, conditionally-updated Cloud Fetch operation state."""

from __future__ import annotations

import base64
import hashlib
import json
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any

from botocore.exceptions import ClientError


@dataclass
class OperationRecord:
    query_id: str
    upstream_handle: str
    metadata: str | None = None
    acknowledged_offset: int = 0
    next_offset: int = 0
    cleanup_at: float | None = None
    updated_at: float = 0

    @property
    def upstream_handle_bytes(self) -> bytes:
        return base64.b64decode(self.upstream_handle)

    @property
    def metadata_bytes(self) -> bytes | None:
        return base64.b64decode(self.metadata) if self.metadata else None


class S3OperationStore:
    """Small CAS records shared by all proxy replicas.

    S3's If-Match/If-None-Match preconditions prevent two replicas from
    silently losing cursor or acknowledgement updates.
    """

    def __init__(
        self,
        s3_client: Any,
        bucket: str,
        prefix: str = "kloudfetch-state",
        max_cas_attempts: int = 20,
    ) -> None:
        self.s3 = s3_client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.max_cas_attempts = max(1, max_cas_attempts)

    def _key(self, handle_key: bytes) -> str:
        digest = hashlib.sha256(handle_key).hexdigest()
        return f"{self.prefix}/operations/{digest}.json"

    @staticmethod
    def _encode(record: OperationRecord) -> bytes:
        return json.dumps(
            asdict(record), separators=(",", ":"), sort_keys=True
        ).encode()

    @staticmethod
    def _decode(body: bytes) -> OperationRecord:
        return OperationRecord(**json.loads(body))

    def load(self, handle_key: bytes) -> tuple[OperationRecord, str] | None:
        try:
            response = self.s3.get_object(
                Bucket=self.bucket, Key=self._key(handle_key)
            )
        except ClientError as error:
            code = error.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404", "NotFound"}:
                return None
            raise
        return self._decode(response["Body"].read()), response["ETag"]

    def create(
        self, handle_key: bytes, query_id: str, upstream_handle: bytes
    ) -> OperationRecord:
        record = OperationRecord(
            query_id=query_id,
            upstream_handle=base64.b64encode(upstream_handle).decode(),
            updated_at=time.time(),
        )
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self._key(handle_key),
                Body=self._encode(record),
                ContentType="application/json",
                ServerSideEncryption="AES256",
                IfNoneMatch="*",
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") not in {
                "PreconditionFailed",
                "ConditionalRequestConflict",
                "412",
            }:
                raise
            existing = self.load(handle_key)
            if existing is None:
                raise
            return existing[0]
        return record

    def update(
        self,
        handle_key: bytes,
        mutate: Callable[[OperationRecord], None],
    ) -> OperationRecord | None:
        for attempt in range(self.max_cas_attempts):
            loaded = self.load(handle_key)
            if loaded is None:
                return None
            record, etag = loaded
            mutate(record)
            record.updated_at = time.time()
            try:
                self.s3.put_object(
                    Bucket=self.bucket,
                    Key=self._key(handle_key),
                    Body=self._encode(record),
                    ContentType="application/json",
                    ServerSideEncryption="AES256",
                    IfMatch=etag,
                )
                return record
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") not in {
                    "PreconditionFailed",
                    "ConditionalRequestConflict",
                    "412",
                }:
                    raise
                if attempt + 1 == self.max_cas_attempts:
                    raise RuntimeError(
                        "operation-state CAS retries exhausted"
                    ) from error
        raise AssertionError("unreachable")

    def delete(self, handle_key: bytes) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=self._key(handle_key))

    def due_for_cleanup(
        self, now: float, abandoned_before: float
    ) -> list[tuple[bytes, OperationRecord]]:
        result: list[tuple[bytes, OperationRecord]] = []
        root = f"{self.prefix}/operations/"
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=root):
            for item in page.get("Contents", []):
                response = self.s3.get_object(
                    Bucket=self.bucket, Key=item["Key"]
                )
                record = self._decode(response["Body"].read())
                if (
                    record.cleanup_at is not None
                    and record.cleanup_at <= now
                ) or record.updated_at < abandoned_before:
                    digest = item["Key"].removeprefix(root).removesuffix(
                        ".json"
                    )
                    result.append((bytes.fromhex(digest), record))
        return result

    def delete_digest(self, digest: bytes) -> None:
        self.s3.delete_object(
            Bucket=self.bucket,
            Key=f"{self.prefix}/operations/{digest.hex()}.json",
        )
