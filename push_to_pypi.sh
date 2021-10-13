#!/usr/bin/env bash

version=$(cat VERSION.txt | tr -d '\040\011\012\015')

rm -rf dist/ &&

python setup.py sdist &&

pushd dist &&
pip install hs-s3fs-${version}.tar.gz --ignore-installed &&

popd &&

python setup.py sdist upload -r https://tools.hivestack.com/