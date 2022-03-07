1. Verify tests on Linux, OS-X, and Windows
2. Run `poetry run bump2version --new-version=YEAR.MONTH.DAY`, replacing `YEAR`, `MONTH` and `DAY` with the current date
3. Run `git push && git push --tags`
4. Run `poetry publish --build`
