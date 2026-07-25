"""Integration check for expired and freshly re-signed S3 result URLs."""

from __future__ import annotations

import os
import time
import urllib.error
import urllib.request
import uuid

import boto3
from botocore.config import Config


def main() -> None:
    endpoint = os.environ.get(
        "KLOUDFETCH_S3_ENDPOINT_URL", "http://rustfs:9000"
    )
    bucket = os.environ.get("KLOUDFETCH_S3_BUCKET", "kloudfetch")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "kloudfetch"),
        aws_secret_access_key=os.environ.get(
            "AWS_SECRET_ACCESS_KEY", "kloudfetch-secret"
        ),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4", s3={"addressing_style": "path"}
        ),
    )
    key = f"expiry-test/{uuid.uuid4().hex}.bin"
    payload = b"freshly-signed-cloud-fetch-link"
    client.put_object(Bucket=bucket, Key=key, Body=payload)
    try:
        expired_url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=1,
        )
        time.sleep(2)
        try:
            urllib.request.urlopen(expired_url, timeout=10).read()
        except urllib.error.HTTPError as error:
            if error.code not in {400, 401, 403}:
                raise
        else:
            raise AssertionError("object store accepted an expired URL")

        fresh_url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=30,
        )
        observed = urllib.request.urlopen(fresh_url, timeout=10).read()
        if observed != payload:
            raise AssertionError("freshly signed URL returned corrupt data")
    finally:
        client.delete_object(Bucket=bucket, Key=key)
    print("Expired URL rejected; freshly re-signed URL succeeded")


if __name__ == "__main__":
    main()
