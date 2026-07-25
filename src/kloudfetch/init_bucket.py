"""Create the development bucket in an S3-compatible object store."""

from __future__ import annotations

import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def main() -> None:
    endpoint = os.environ.get("KLOUDFETCH_S3_ENDPOINT_URL", "http://rustfs:9000")
    bucket = os.environ.get("KLOUDFETCH_S3_BUCKET", "kloudfetch")
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "kloudfetch"),
        aws_secret_access_key=os.environ.get(
            "AWS_SECRET_ACCESS_KEY", "kloudfetch-secret"
        ),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    for attempt in range(60):
        try:
            client.head_bucket(Bucket=bucket)
            break
        except client.exceptions.ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404:
                client.create_bucket(Bucket=bucket)
                break
        except Exception:
            if attempt == 59:
                raise
        time.sleep(1)
    else:
        raise RuntimeError(f"object-store bucket {bucket!r} did not become ready")

    lifecycle_days = int(os.environ.get("KLOUDFETCH_LIFECYCLE_DAYS", "1"))
    if lifecycle_days <= 0:
        return
    prefix = os.environ.get("KLOUDFETCH_S3_PREFIX", "results").strip("/") + "/"
    try:
        client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "kloudfetch-expire-results",
                        "Status": "Enabled",
                        "Filter": {"Prefix": prefix},
                        "Expiration": {"Days": lifecycle_days},
                        "AbortIncompleteMultipartUpload": {
                            "DaysAfterInitiation": 1
                        },
                    }
                ]
            },
        )
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code not in {"NotImplemented", "XNotImplemented"}:
            raise
        print(
            "object store does not support lifecycle configuration; "
            "proxy cleanup remains active",
            flush=True,
        )


if __name__ == "__main__":
    main()
