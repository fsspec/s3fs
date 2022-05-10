1.  Verify tests on Linux, OS-X, and Windows

2.  Complete entries in `docs/source/changelog.rst`.

    There's no need for changing version numbers in source files.
    The release version will be determined from the git tag (see below).

3.  Tag the commit

        git tag 1.2.3 -m "Version 1.2.3"

4.  Push new version bump commit and tag to github

        git push fsspec main --tags

5.  Build source and wheel packages

        rm -rf dist/
        python setup.py sdist bdist_wheel --universal

6.  Upload packages to PyPI

        twine upload dist/*
