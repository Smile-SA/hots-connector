# Contributing

Issues and pull requests are welcome on
[GitHub](https://github.com/Smile-SA/hots-connector).

## Development setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Before opening a pull request, run the same checks as CI:

```bash
ruff check hots_connector tests
mypy hots_connector
pytest
```

## Changing the message contract

The Kafka messages exchanged with HOTS are pinned by the fixtures in
`tests/contracts/fixtures/`, which must stay byte-identical with HOTS's copy.
A change to either message shape is a change in both repositories: see
`tests/contracts/README.md`.

## Releasing

1. Set the new version in `VERSION.txt`, and move the `CHANGELOG.md` entries
   under it.
2. Merge to `main`, then push a tag `v<version>` that matches `VERSION.txt`.
3. The `Release` workflow builds the package and publishes it to PyPI.
