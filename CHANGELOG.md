# Changelog

All notable changes to hots-connector are listed here. Versions follow
[semantic versioning](https://semver.org/); while the version is 0.x, a minor
release may change settings or the HTTP API.

## 0.1.0 (YYYY-MM-DD)

First public release.

- Pluggable backends: Prometheus observation (`prometheus_docker`,
  `prometheus_kubernetes` profiles), Kafka transport, Docker (`recreate`) and
  Kubernetes (nodeSelector patch + eviction) actions.
- CLI: `hots-connector api|run|check` and `--version`.
- HTTP API: `/health`, `/check`, `/stream/start`, `/stream/stop`,
  `/kubernetes/reset-node-selectors`.
- Message contract with HOTS, schema `v: 1`, pinned by the fixtures in
  `tests/contracts/fixtures/`. Works with HOTS 2.1 or later.
- `HOTSCONNECTOR_ENV_FILE` chooses the dotenv file the CLI reads; empty reads
  none.
- `hots_connector.__version__` comes from the installed package metadata.
