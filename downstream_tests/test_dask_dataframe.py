import pytest
from pytest import fixture
import pandas as pd
import numpy as np
import time
import pyarrow as pa
import dask.dataframe as dd
import s3fs
import moto.server
import sys
import os
from typing import List


@fixture(scope="session", autouse=True)
def aws_credentials():
    os.environ["BOTO_CONFIG"] = "/dev/null"
    os.environ["AWS_ACCESS_KEY_ID"] = "foobar_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "foobar_secret"


@fixture(scope="session")
def partitioned_dataset() -> dict:
    rows = 500000
    cds_df = pd.DataFrame(
        {
            "id": range(rows),
            "part_key": np.random.choice(["A", "B", "C", "D"], rows),
            "timestamp": np.random.randint(1051638817, 1551638817, rows),
            "int_value": np.random.randint(0, 60000, rows),
        }
    )
    return {"dataframe": cds_df, "partitioning_column": "part_key"}


def free_port():
    import socketserver

    with socketserver.TCPServer(("localhost", 0), None) as s:
        free_port = s.server_address[1]
        return free_port


@fixture(scope="session")
def moto_server(aws_credentials):
    import subprocess

    port = free_port()
    process = subprocess.Popen([
        sys.executable,
        moto.server.__file__,
        '--port', str(port),
        '--host', 'localhost',
        's3'
    ])

    s3fs_kwargs = dict(
        client_kwargs={"endpoint_url": f'http://localhost:{port}'},
    )

    start = time.time()
    while True:
        try:
            fs = s3fs.S3FileSystem(skip_instance_cache=True, **s3fs_kwargs)
            fs.ls("/")
        except:
            if time.time() - start > 30:
                raise TimeoutError("Could not get a working moto server in time")
        time.sleep(0.1)

        break

    yield s3fs_kwargs

    process.terminate()


@fixture(scope="session")
def moto_s3fs(moto_server):
    return s3fs.S3FileSystem(**moto_server)


@fixture(scope="session")
def s3_bucket(moto_server):
    test_bucket_name = 'test'
    from botocore.session import Session
    # NB: we use the sync botocore client for setup
    session = Session()
    client = session.create_client('s3', **moto_server['client_kwargs'])
    client.create_bucket(Bucket=test_bucket_name, ACL='public-read')
    return test_bucket_name


@fixture(scope="session")
def partitioned_parquet_path(partitioned_dataset, moto_s3fs, s3_bucket):
    cds_df = partitioned_dataset["dataframe"]
    table = pa.Table.from_pandas(cds_df, preserve_index=False)
    path = s3_bucket + "/partitioned/dataset"
    import pyarrow.parquet

    pyarrow.parquet.write_to_dataset(
        table,
        path,
        filesystem=moto_s3fs,
        partition_cols=[
            partitioned_dataset["partitioning_column"]
        ],  # new parameter included
    )

    # storage_options = dict(use_listings_cache=False)
    # storage_options.update(docker_aws_s3.s3fs_kwargs)
    #
    # import dask.dataframe
    #
    # ddf = dask.dataframe.read_parquet(
    #     f"s3://{path}", storage_options=storage_options, gather_statistics=False
    # )
    # all_rows = ddf.compute()
    # assert "name" in all_rows.columns
    return path


@pytest.fixture(scope='session', params=[
    pytest.param("pyarrow"),
    pytest.param("fastparquet"),
])
def parquet_engine(request):
    return request.param


@pytest.fixture(scope='session', params=[
    pytest.param(False, id='gather_statistics=F'),
    pytest.param(True, id='gather_statistics=T'),
    pytest.param(None, id='gather_statistics=None'),
])
def gather_statistics(request):
    return request.param


def compare_dateframes(actual: pd.DataFrame, expected: pd.DataFrame, columns: List[str], id_column='id'):
    from pandas.testing import assert_frame_equal

    if id_column in columns:
        columns.remove(id_column)

    actual = actual.set_index(id_column)
    expected = expected.set_index(id_column)

    assert_frame_equal(actual.loc[:, columns], expected.loc[:, columns], check_dtype=False)


def test_partitioned_read(partitioned_dataset, partitioned_parquet_path, moto_server, parquet_engine, gather_statistics):
    """The directory based reading is quite finicky"""
    storage_options = moto_server.copy()
    ddf = dd.read_parquet(
        f"s3://{partitioned_parquet_path}",
        storage_options=storage_options,
        gather_statistics=gather_statistics,
        engine=parquet_engine
    )

    assert 'part_key' in ddf.columns
    actual = ddf.compute().sort_values('id')
    # dask converts our part_key into a categorical
    actual[:, 'part_key'] = actual[:, 'part_key'].astype(str)


    column_names = list(partitioned_dataset["dataframe"].columns)

    compare_dateframes(actual, partitioned_dataset["dataframe"], column_names)


def test_non_partitioned_read(partitioned_dataset, partitioned_parquet_path, moto_server, parquet_engine, gather_statistics):
    """The directory based reading is quite finicky"""
    storage_options = moto_server.copy()
    ddf = dd.read_parquet(
        f"s3://{partitioned_parquet_path}/part_key=A",
        storage_options=storage_options,
        gather_statistics=gather_statistics,
        engine=parquet_engine
    )

    # if parquet_engine == 'pyarrow':
    #     assert 'part_key' in ddf.columns

    actual = ddf.compute().sort_values('id')
    expected: pd.DataFrame = partitioned_dataset["dataframe"]
    expected = expected.loc[expected.part_key == "A"]
    expected = expected.drop(columns=['part_key'])

    compare_dateframes(actual, expected, list(expected.columns))
