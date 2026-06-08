#!/usr/bin/env python
"""Benchmark single-file download throughput for s3fs.

Point it at an S3 bucket/prefix and, in one go, it can ``--generate`` random
test files of varying sizes, upload them, and then benchmark downloading them.
Each object is downloaded several times; the median wall-clock time and
throughput (MB/s) are reported. Optionally it:

* compares against the AWS CLI (``aws s3 cp``) on the same objects,
* sweeps ``max_concurrency`` / ``chunksize`` to show how tuning affects s3fs
  (the point of the default-speed work is that the *out-of-the-box* numbers
  should already be close to ``aws s3 cp``, with no sweep needed), and
* ``--cleanup`` the objects it generated when finished.

Examples
--------
End-to-end against an AWS bucket: generate + upload + benchmark + clean up::

    python benchmarks/benchmark_download.py \\
        --s3-prefix s3://my-bucket/s3fs-bench \\
        --generate --aws-cli --cleanup

Benchmark objects that already exist under the prefix::

    python benchmarks/benchmark_download.py \\
        --s3-prefix s3://my-bucket/s3fs-bench --aws-cli

Sweep concurrency to see the effect of tuning::

    python benchmarks/benchmark_download.py \\
        --s3-prefix s3://my-bucket/s3fs-bench \\
        --max-concurrency 1 5 10 20

Generate a custom set of sizes against MinIO::

    python benchmarks/benchmark_download.py \\
        --s3-prefix s3://my-bucket/s3fs-bench --generate \\
        --sizes 8MB 64MB 256MB 1GB \\
        --endpoint-url http://localhost:9000
"""

import argparse
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time

# Allow importing the sibling generator module regardless of how this script is
# launched (as a path, or as ``python -m benchmarks.benchmark_download``).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_test_files import (  # noqa: E402
    DEFAULT_SIZES,
    parse_size,
    size_label,
    write_random_file,
)


def human_size(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024


def throughput_mbps(nbytes, seconds):
    if seconds <= 0:
        return float("inf")
    return (nbytes / 2**20) / seconds


def time_runs(fn, runs):
    """Run ``fn`` ``runs`` times, return (median, best) elapsed seconds."""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), min(times)


def make_s3fs(endpoint_url):
    import s3fs

    client_kwargs = {"endpoint_url": endpoint_url} if endpoint_url else {}
    # New instance per call so the instance cache / sessions don't carry state
    # between configurations being compared.
    s3fs.S3FileSystem.clear_instance_cache()
    return s3fs.S3FileSystem(client_kwargs=client_kwargs)


def bench_s3fs(s3, src, dst, max_concurrency, chunksize):
    kwargs = {}
    if max_concurrency is not None:
        kwargs["max_concurrency"] = max_concurrency
    if chunksize is not None:
        kwargs["chunksize"] = chunksize

    def run():
        s3.get_file(src, dst, **kwargs)

    return run


def bench_aws_cli(src, dst, endpoint_url):
    cmd = ["aws", "s3", "cp", src, dst, "--quiet"]
    if endpoint_url:
        cmd += ["--endpoint-url", endpoint_url]

    def run():
        subprocess.run(cmd, check=True)

    return run


def parse_chunksize(text):
    if text is None:
        return None
    s = text.strip().upper()
    for unit, mult in (("MIB", 2**20), ("MB", 2**20), ("KIB", 2**10), ("KB", 2**10)):
        if s.endswith(unit):
            return int(float(s[: -len(unit)]) * mult)
    return int(s)


def generate_objects(s3, prefix, sizes, name_prefix, overwrite):
    """Create + upload random test objects under ``prefix``.

    Returns the list of object paths that were newly created (so the caller can
    optionally clean them up). Random data is streamed straight to S3, so no
    local disk space or large in-memory buffer is needed.
    """
    created = []
    for spec in sizes:
        label = size_label(spec)
        nbytes = parse_size(spec)
        s3_path = f"{prefix}/{name_prefix}_{label}.bin"
        if s3.exists(s3_path) and not overwrite:
            print(f"skip   upload {s3_path} (exists)")
            continue
        t0 = time.perf_counter()
        with s3.open(s3_path, "wb") as f:
            write_random_file(f, nbytes)
        dt = time.perf_counter() - t0
        print(f"upload {s3_path} ({human_size(nbytes)}, {dt:.1f}s)")
        created.append(s3_path)
    return created


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Benchmark s3fs single-file download throughput.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--s3-prefix",
        required=True,
        help="S3 prefix holding the benchmark objects, e.g. s3://bucket/s3fs-bench.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("AWS_ENDPOINT_URL"),
        help="Custom S3 endpoint (e.g. MinIO). Defaults to $AWS_ENDPOINT_URL.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of timed runs per object/configuration (median is reported).",
    )
    parser.add_argument(
        "--max-concurrency",
        nargs="+",
        type=int,
        default=[None],
        help="One or more max_concurrency values to test (default: library default).",
    )
    parser.add_argument(
        "--chunksize",
        default=None,
        help="Override transfer chunk size, e.g. 8MB (default: library default).",
    )
    parser.add_argument(
        "--aws-cli",
        action="store_true",
        help="Also benchmark `aws s3 cp` on the same objects for comparison.",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate and upload random test files to the prefix before benchmarking.",
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        default=DEFAULT_SIZES,
        help="Sizes to generate with --generate (e.g. 8MB 64MB 1GB).",
    )
    parser.add_argument(
        "--name-prefix",
        default="testfile",
        help="Base name for generated files.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="With --generate, re-create objects that already exist.",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete the objects generated by this run when finished.",
    )
    args = parser.parse_args(argv)

    if args.cleanup and not args.generate:
        parser.error("--cleanup only applies to objects created by --generate")
    if args.aws_cli and shutil.which("aws") is None:
        parser.error("--aws-cli requested but the `aws` CLI is not on PATH")

    chunksize = parse_chunksize(args.chunksize)
    s3 = make_s3fs(args.endpoint_url)

    prefix = args.s3_prefix.rstrip("/")

    created = []
    if args.generate:
        created = generate_objects(
            s3, prefix, args.sizes, args.name_prefix, args.overwrite
        )
        s3.invalidate_cache()
        print()

    objects = [p for p in s3.find(prefix) if not p.endswith("/")]
    if not objects:
        parser.error(f"no objects found under {prefix!r}")
    sizes = {p: s3.info(p)["size"] for p in objects}
    objects.sort(key=lambda p: sizes[p])

    # Build the list of (label, runner-factory) configurations to compare.
    configs = []
    for mc in args.max_concurrency:
        label = "s3fs default" if mc is None and chunksize is None else "s3fs"
        details = []
        if mc is not None:
            details.append(f"mc={mc}")
        if chunksize is not None:
            details.append(f"chunk={human_size(chunksize)}")
        if details:
            label = f"s3fs ({', '.join(details)})"
        configs.append((label, mc))

    header = f"{'object':<28}{'size':>10}{'config':>22}{'median':>10}{'MB/s':>10}"
    print(header)
    print("-" * len(header))

    tmpdir = tempfile.mkdtemp(prefix="s3fs-bench-")
    try:
        for obj in objects:
            nbytes = sizes[obj]
            name = obj.rsplit("/", 1)[-1]
            src = obj if obj.startswith("s3://") else f"s3://{obj}"

            for label, mc in configs:
                dst = os.path.join(tmpdir, name)
                runner = bench_s3fs(s3, src, dst, mc, chunksize)
                median, best = time_runs(runner, args.runs)
                _verify(dst, nbytes)
                print(
                    f"{name:<28}{human_size(nbytes):>10}{label:>22}"
                    f"{median:>9.2f}s{throughput_mbps(nbytes, best):>10.1f}"
                )

            if args.aws_cli:
                dst = os.path.join(tmpdir, name)
                runner = bench_aws_cli(src, dst, args.endpoint_url)
                median, best = time_runs(runner, args.runs)
                _verify(dst, nbytes)
                print(
                    f"{name:<28}{human_size(nbytes):>10}{'aws s3 cp':>22}"
                    f"{median:>9.2f}s{throughput_mbps(nbytes, best):>10.1f}"
                )
            print()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    if args.cleanup and created:
        for path in created:
            try:
                s3.rm(path)
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                print(f"warning: failed to delete {path}: {exc}")
        print(f"cleaned up {len(created)} generated object(s)")

    return 0


def _verify(path, nbytes):
    actual = os.path.getsize(path)
    if actual != nbytes:
        raise SystemExit(
            f"download integrity check failed for {path}: "
            f"got {actual} bytes, expected {nbytes}"
        )


if __name__ == "__main__":
    sys.exit(main())
