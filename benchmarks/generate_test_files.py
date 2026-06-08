#!/usr/bin/env python
"""Generate test files of varying sizes for download benchmarks.

The files are written with random (incompressible) content so that they cannot
be transparently compressed on the wire, giving a realistic picture of transfer
throughput.

Files can be written to a local directory and/or uploaded directly to S3 (via
s3fs), which is what ``benchmark_download.py`` expects.

Examples
--------
Generate the default set of sizes locally::

    python benchmarks/generate_test_files.py --local-dir /tmp/s3fs-bench

Upload a custom set of sizes straight to S3::

    python benchmarks/generate_test_files.py \\
        --s3-prefix s3://my-bucket/s3fs-bench \\
        --sizes 8MB 64MB 256MB 1GB

Point at a non-AWS endpoint (e.g. MinIO)::

    python benchmarks/generate_test_files.py \\
        --s3-prefix s3://my-bucket/s3fs-bench \\
        --endpoint-url http://localhost:9000
"""

import argparse
import os
import sys
import time

# Default set of sizes. Chosen to straddle the 8 MiB transfer-chunk default so
# both the sequential (small file) and concurrent (large file) paths are
# exercised, up to sizes where parallelism dominates.
DEFAULT_SIZES = ["1MB", "8MB", "16MB", "64MB", "128MB", "256MB", "512MB", "1GB"]

_UNITS = {
    "B": 1,
    "KB": 2**10,
    "KIB": 2**10,
    "MB": 2**20,
    "MIB": 2**20,
    "GB": 2**30,
    "GIB": 2**30,
}


def parse_size(text):
    """Parse a human size like ``64MB`` / ``1GiB`` / ``1048576`` into bytes."""
    s = text.strip().upper()
    for unit in ("KIB", "MIB", "GIB", "KB", "MB", "GB", "B"):
        if s.endswith(unit):
            number = s[: -len(unit)].strip()
            return int(float(number) * _UNITS[unit])
    return int(s)  # bare number of bytes


def human_size(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024


def size_label(text):
    """Normalise a size spec into a filesystem-friendly label, e.g. ``64MB``."""
    return text.strip().upper().replace("IB", "iB")


def write_random_file(fileobj, nbytes, block=8 * 2**20):
    """Stream ``nbytes`` of random data into ``fileobj`` without buffering it all."""
    remaining = nbytes
    while remaining > 0:
        chunk = os.urandom(min(block, remaining))
        fileobj.write(chunk)
        remaining -= len(chunk)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Generate random test files of varying sizes for benchmarks.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        default=DEFAULT_SIZES,
        help="Sizes to generate (e.g. 8MB 64MB 1GB or a bare byte count).",
    )
    parser.add_argument(
        "--local-dir",
        help="Directory to write files to. Created if missing.",
    )
    parser.add_argument(
        "--s3-prefix",
        help="S3 prefix to upload files to, e.g. s3://my-bucket/s3fs-bench.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("AWS_ENDPOINT_URL"),
        help="Custom S3 endpoint (e.g. MinIO). Defaults to $AWS_ENDPOINT_URL.",
    )
    parser.add_argument(
        "--prefix",
        default="testfile",
        help="Base name for generated files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Re-create files/objects that already exist.",
    )
    args = parser.parse_args(argv)

    if not args.local_dir and not args.s3_prefix:
        parser.error("provide --local-dir and/or --s3-prefix")

    sizes = [(size_label(s), parse_size(s)) for s in args.sizes]

    s3 = None
    if args.s3_prefix:
        import s3fs

        client_kwargs = {"endpoint_url": args.endpoint_url} if args.endpoint_url else {}
        s3 = s3fs.S3FileSystem(client_kwargs=client_kwargs)

    if args.local_dir:
        os.makedirs(args.local_dir, exist_ok=True)

    for label, nbytes in sizes:
        name = f"{args.prefix}_{label}.bin"
        local_path = os.path.join(args.local_dir, name) if args.local_dir else None
        s3_path = f"{args.s3_prefix.rstrip('/')}/{name}" if args.s3_prefix else None

        # Local file.
        if local_path:
            if os.path.exists(local_path) and not args.overwrite:
                print(f"skip   local {local_path} (exists)")
            else:
                t0 = time.perf_counter()
                with open(local_path, "wb") as f:
                    write_random_file(f, nbytes)
                dt = time.perf_counter() - t0
                print(f"wrote  local {local_path} ({human_size(nbytes)}, {dt:.1f}s)")

        # S3 object.
        if s3_path:
            if s3.exists(s3_path) and not args.overwrite:
                print(f"skip   s3    {s3_path} (exists)")
            elif local_path and os.path.exists(local_path):
                t0 = time.perf_counter()
                s3.put(local_path, s3_path)
                dt = time.perf_counter() - t0
                print(f"upload s3    {s3_path} ({human_size(nbytes)}, {dt:.1f}s)")
            else:
                # No local copy: stream random data straight to the object.
                t0 = time.perf_counter()
                with s3.open(s3_path, "wb") as f:
                    write_random_file(f, nbytes)
                dt = time.perf_counter() - t0
                print(f"upload s3    {s3_path} ({human_size(nbytes)}, {dt:.1f}s)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
