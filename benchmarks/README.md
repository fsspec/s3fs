# Download benchmarks

Small scripts to measure single-file download throughput and confirm that s3fs
downloads are fast out-of-the-box — comparable to `aws s3 cp` without tuning
`max_concurrency` or `chunksize`.

> These run against real S3 (or a MinIO endpoint), not moto, since moto has no
> network latency and so cannot show transfer throughput.

## 1. Generate test files

```bash
# Straight to S3 (a range of sizes around the 8 MiB transfer-chunk default)
python benchmarks/generate_test_files.py \
    --s3-prefix s3://my-bucket/s3fs-bench \
    --sizes 1MB 8MB 64MB 256MB 1GB

# Or write locally first, then upload
python benchmarks/generate_test_files.py \
    --local-dir /tmp/s3fs-bench --s3-prefix s3://my-bucket/s3fs-bench
```

Files contain random (incompressible) data so throughput is realistic.

## 2. Benchmark downloads

```bash
# s3fs defaults vs. the AWS CLI on the same objects
python benchmarks/benchmark_download.py \
    --s3-prefix s3://my-bucket/s3fs-bench --aws-cli

# Sweep concurrency to see the effect of tuning
python benchmarks/benchmark_download.py \
    --s3-prefix s3://my-bucket/s3fs-bench \
    --max-concurrency 1 5 10 20
```

Each object is downloaded `--runs` times (default 3); the median time and the
best-case throughput (MB/s) are reported, and every download is size-checked.

## MinIO / custom endpoints

Both scripts accept `--endpoint-url` (or honour `$AWS_ENDPOINT_URL`):

```bash
export AWS_ENDPOINT_URL=http://localhost:9000
python benchmarks/generate_test_files.py --s3-prefix s3://bucket/s3fs-bench
python benchmarks/benchmark_download.py  --s3-prefix s3://bucket/s3fs-bench --aws-cli
```

Credentials are picked up from the standard AWS sources (env vars, shared
config/credentials files, instance role, etc.).
