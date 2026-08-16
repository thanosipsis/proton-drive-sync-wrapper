# Contributing

Keep changes focused and preserve the safety invariants documented in
`docs/architecture.md`. New behavior needs tests, and provider adapters need
contract plus disposable-canary coverage.

Before opening a pull request:

```bash
ruff check .
mypy src/verified_mirror --ignore-missing-imports
bandit -q -r src
PYTHONPATH=src python -m unittest discover -s tests -v
python -m build
```

Never include real credentials, account identifiers, private remote paths, or
production state databases in issues, fixtures, logs, or commits.
