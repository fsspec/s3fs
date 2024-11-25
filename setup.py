#!/usr/bin/env python

from setuptools import setup
import versioneer

with open("requirements.txt") as file:
    aiobotocore_version_suffix = ""
    for line in file:
        parts = line.rstrip().split("aiobotocore")
        if len(parts) == 2:
            aiobotocore_version_suffix = parts[1]
            break

setup(
    name="s3fs",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    description="Convenient Filesystem interface over S3",
    url="http://github.com/fsspec/s3fs/",
    maintainer="Martin Durant",
    maintainer_email="mdurant@continuum.io",
    license="BSD",
    keywords="s3, boto",
    packages=["s3fs"],
    python_requires=">= 3.9",
    install_requires=[open("requirements.txt").read().strip().split("\n")],
    extras_require={
        "awscli": [f"aiobotocore[awscli]{aiobotocore_version_suffix}"],
        "boto3": [f"aiobotocore[boto3]{aiobotocore_version_suffix}"],
    },
    zip_safe=False,
)
