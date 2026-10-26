"""Download pinned JVM dependencies for LOCAL tests, verifying their SHA-256 digests."""

import hashlib
import urllib.request
from pathlib import Path

DEPENDENCIES = [
    (
        "org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.11.0/",
        "iceberg-spark-runtime-4.1_2.13-1.11.0.jar",
        "d6ea6c5d099288daeb7d5a92061bd3d7d8f296492632b42378e5f2f0e3066242",
    ),
    (
        "com/h2database/h2/2.4.240/",
        "h2-2.4.240.jar",
        "29b70e427cc1c40cdc376283adbb0cc62853073797bb5fe5761f81fe73d57ce0",
    ),
]


def main():
    for directory, filename, checksum in DEPENDENCIES:
        output = Path("artifacts") / filename
        if not output.exists():
            output.parent.mkdir(parents=True, exist_ok=True)
            url = f"https://repo.maven.apache.org/maven2/{directory}{filename}"
            with urllib.request.urlopen(url, timeout=60) as response:
                content = response.read()
        else:
            content = output.read_bytes()
        if hashlib.sha256(content).hexdigest() != checksum:
            raise ValueError(f"Checksum mismatch for {filename}; refusing to load JVM code")
        output.write_bytes(content)
        print(output)


if __name__ == "__main__":
    main()
