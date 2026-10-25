"""Small storage boundary. Local mode never constructs an AWS client."""

import os
import tempfile
from pathlib import Path, PurePosixPath
from typing import Protocol


def safe_key(key: str) -> str:
    path = PurePosixPath(key)
    if path.is_absolute() or ".." in path.parts or not key or "\\" in key:
        raise ValueError(f"Unsafe object key: {key!r}")
    return key


class Store(Protocol):
    def read(self, key: str) -> bytes: ...
    def write(self, key: str, content: bytes, *, immutable: bool = False) -> None: ...
    def exists(self, key: str) -> bool: ...
    def uri(self, key: str) -> str: ...


class LocalStore:
    def __init__(self, root: Path):
        self.root = root.resolve()

    def path(self, key):
        path = (self.root / safe_key(key)).resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("Object path escapes local root")
        return path

    def read(self, key):
        return self.path(key).read_bytes()

    def exists(self, key):
        return self.path(key).exists()

    def uri(self, key):
        return str(self.path(key))

    def write(self, key, content, *, immutable=False):
        path = self.path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        if immutable and path.exists():
            if path.read_bytes() != content:
                raise ValueError(f"Immutable object conflict: {key}")
            return
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            if immutable:
                try:
                    os.link(temporary, path)
                except FileExistsError:
                    if path.read_bytes() != content:
                        raise ValueError(f"Immutable object conflict: {key}") from None
            else:
                os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


class S3Store:
    def __init__(self, bucket: str, region: str, *, client=None):
        if client is None:
            import boto3
            from botocore.config import Config

            client = boto3.client(
                "s3",
                region_name=region,
                config=Config(retries={"max_attempts": 3, "mode": "standard"}),
            )
        self.client = client
        self.bucket = bucket

    def uri(self, key):
        return f"s3://{self.bucket}/{safe_key(key)}"

    def read(self, key):
        response = self.client.get_object(Bucket=self.bucket, Key=safe_key(key))
        try:
            return response["Body"].read()
        finally:
            response["Body"].close()

    def exists(self, key):
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=safe_key(key))
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def write(self, key, content, *, immutable=False):
        from botocore.exceptions import ClientError

        arguments = {"Bucket": self.bucket, "Key": safe_key(key), "Body": content}
        if immutable:
            arguments["IfNoneMatch"] = "*"
        try:
            # Respect the bucket's existing default encryption; never change bucket settings.
            self.client.put_object(**arguments)
        except ClientError as exc:
            if not immutable or exc.response["Error"]["Code"] != "PreconditionFailed":
                raise
            if self.read(key) != content:
                raise ValueError(f"Immutable object conflict: {key}") from exc
