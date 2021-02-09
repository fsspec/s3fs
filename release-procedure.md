1.  Verify tests on Linux, OS-X, and Windows

2.  Update version in setup.py and s3fs/__init__.py and commit

3.  Tag the commit

        git tag 1.2.3 -m "Version 1.2.3"

4.  Push new version bump commit and tag to github

        git push dask main --tags

5.  Build source and wheel packages

        rm -rf dist/
        python setup.py sdist bdist_wheel --universal

6.  Upload packages to PyPI

        twine upload dist/*
