#!/usr/bin/env python

from setuptools import setup
import versioneer

setup(
    name="s3fs",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
    description="Convenient Filesystem interface over S3",
    url="http://github.com/fsspec/s3fs/",
    maintainer="Martin Durant",
    maintainer_email="mdurant@continuum.io",
    license="BSD",
    keywords="s3, boto",
    packages=["s3fs"],
    python_requires=">= 3.10",
    install_requires=[open("requirements.txt").read().strip().split("\n")],
    long_description="README.md",
    long_description_content_type="text/markdown",
    zip_safe=False,
)
