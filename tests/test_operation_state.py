import io
import json
from hashlib import md5

from botocore.exceptions import ClientError

from kloudfetch.operation_state import S3OperationStore


class FakePaginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, Bucket, Prefix):
        del Bucket
        return [
            {
                "Contents": [
                    {"Key": key}
                    for key in sorted(self.client.objects)
                    if key.startswith(Prefix)
                ]
            }
        ]


class FakeS3:
    def __init__(self):
        self.objects = {}

    @staticmethod
    def error(code, operation):
        raise ClientError(
            {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": 412}},
            operation,
        )

    def put_object(self, Bucket, Key, Body, IfMatch=None, IfNoneMatch=None, **kwargs):
        del Bucket, kwargs
        current = self.objects.get(Key)
        if IfNoneMatch == "*" and current is not None:
            self.error("PreconditionFailed", "PutObject")
        if IfMatch is not None and (
            current is None or current[1] != IfMatch
        ):
            self.error("PreconditionFailed", "PutObject")
        body = bytes(Body)
        etag = f'"{md5(body, usedforsecurity=False).hexdigest()}"'
        self.objects[Key] = (body, etag)

    def get_object(self, Bucket, Key):
        del Bucket
        if Key not in self.objects:
            self.error("NoSuchKey", "GetObject")
        body, etag = self.objects[Key]
        return {"Body": io.BytesIO(body), "ETag": etag}

    def delete_object(self, Bucket, Key):
        del Bucket
        self.objects.pop(Key, None)

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator(self)


def test_operation_state_survives_store_recreation_and_tracks_cursor():
    s3 = FakeS3()
    handle = b"driver-visible-handle"
    first_proxy = S3OperationStore(s3, "bucket")
    first_proxy.create(handle, "query-1", b"upstream-handle")
    first_proxy.update(
        handle,
        lambda record: (
            setattr(record, "next_offset", 42),
            setattr(record, "acknowledged_offset", 12),
        ),
    )

    restarted_proxy = S3OperationStore(s3, "bucket")
    record, _ = restarted_proxy.load(handle)

    assert record.query_id == "query-1"
    assert record.upstream_handle_bytes == b"upstream-handle"
    assert record.next_offset == 42
    assert record.acknowledged_offset == 12


def test_operation_state_uses_encrypted_json_and_cleanup_deadlines():
    s3 = FakeS3()
    store = S3OperationStore(s3, "bucket")
    handle = b"operation"
    store.create(handle, "query-2", b"upstream")
    store.update(
        handle,
        lambda record: setattr(record, "cleanup_at", 100.0),
    )
    due = store.due_for_cleanup(now=101.0, abandoned_before=0.0)

    assert len(due) == 1
    assert due[0][1].query_id == "query-2"
    stored = next(iter(s3.objects.values()))[0]
    assert json.loads(stored)["cleanup_at"] == 100.0
