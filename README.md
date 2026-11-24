# hots-connector

Bridge between [HOTS](https://github.com/Smile-SA/hots) and a real environment
(currently Docker or Kubernetes). It observes workload metrics from
Prometheus, publishes them to HOTS over Kafka, consumes HOTS's placement
decisions from the same broker, and applies them as environment-specific
actions (recreate a container on a new Docker host, or patch+evict a
Kubernetes pod onto a new node).

## Install

```bash
pip install "hots-connector[kafka,kubernetes]"   # Kubernetes action backend
pip install "hots-connector[kafka,docker]"       # Docker action backend
hots-connector --version
```

Each backend's client library is an optional extra (`kafka`, `docker`,
`kubernetes`): install the ones your configuration selects. Python 3.10 or
later.

## Compatibility

| hots-connector | HOTS | Message schema |
| --- | --- | --- |
| 0.1.x | 2.1 or later (`connector.name = "kafka"`) | `v: 1` |

HOTS's side of the setup is its Kafka connector config:
`connector.parameters.topic_data` and `topic_moves` must match
`HOTSCONNECTOR_KAFKA_TOPIC_DATA` and `HOTSCONNECTOR_KAFKA_TOPIC_MOVES`. See
"Message contract with HOTS" below for the wire format.

## Architecture

Three pluggable backends, each behind an abstract interface and a factory
picked by `Settings`:

- **`ObservationBackend`** (`hots_connector/observation/`) -- collects a
  `WorkloadBatch` of `WorkloadSample`s. Currently one implementation,
  `PrometheusObservationBackend`, parameterized by an environment *profile*
  (`prometheus_docker` or `prometheus_kubernetes`) that supplies the right
  default PromQL queries and label names; override any of them explicitly
  via settings.
- **`DecisionTransport`** (`hots_connector/transport/`) -- publishes
  telemetry and consumes `PlacementMove`s. Currently `kafka`
  (`KafkaTransport`).
- **`ActionBackend`** (`hots_connector/actions/`) -- applies a
  `PlacementMove` to the real environment. Currently `docker`
  (`DockerActionBackend`, recreate-based) and `kubernetes`
  (`KubernetesActionBackend`, nodeSelector-patch + eviction, v0).

`ConnectorService` (`hots_connector/services/connector_service.py`) ties the
three together into an observe -> publish -> consume -> apply tick loop, and
is the object shared by both entry points:

- **CLI** (`hots-connector api|run|check`) -- see below.
- **HTTP API** (FastAPI, `hots_connector/api/`) -- see endpoints below.

## Dev install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

The base install has no Docker/Kafka/Kubernetes client libraries -- each
backend's client library is an optional extra (`docker`, `kafka`,
`kubernetes`) so a deployment only pulls in what it actually uses. `dev`
installs all three so the full test suite (including the ones that stub
their SDKs) can run locally.

## CLI

```bash
hots-connector api [--host 127.0.0.1] [--port 8000]   # run the HTTP API (uvicorn)
hots-connector run                                     # run the tick loop, no HTTP
hots-connector check                                    # validate connectivity to all three backends, print a report, exit
hots-connector --version
```

## HTTP API

| Method & path | Description |
| --- | --- |
| `GET /health` | Liveness check; always `{"status": "ok"}`. |
| `GET /check` | Runs `Settings`-configured backends' `validate()` and returns a per-backend `ok` / `error: ...` report. |
| `POST /stream/start` | Starts the tick loop on a background thread. Idempotent: returns `{"status": "already_running"}` if one is already active. |
| `POST /stream/stop` | Signals the loop to stop and joins the thread (5s bound). Returns `{"status": "stopped"}`, or `{"status": "stopping"}` if the current tick didn't finish within the bound. |
| `POST /kubernetes/reset-node-selectors` | Restores every Deployment `KubernetesActionBackend` has patched (this process) back to its pre-run `nodeSelector`. `{"status": "not_supported"}` on any other action backend. See "nodeSelector is sticky" below. |

### Security

The HTTP API has **no authentication**, and its endpoints start a loop that
moves workloads and patches Deployments. It binds to `127.0.0.1` by default:
keep it there, or put it behind something that authenticates callers. Do not
pass `--host 0.0.0.0` on a reachable network.

## Message contract with HOTS

The wire formats are pinned by golden fixtures shared with the HOTS repo,
under `tests/contracts/fixtures/` (copied byte-identical from HOTS's own
`tests/contracts/fixtures/` -- see `tests/contracts/README.md`). Summary:

**Telemetry** (`hots-connector` -> HOTS, on `kafka_topic_data`): one Kafka
message per batch, `{"v": 1, "containers": [...]}` -- no other top-level
key. Each container entry: `{"timestamp": int, "container_id": str, "machine_id": str, "cpu": float, ...}`
(a metric key is present only when the value was observed; a container
missing a metric is *omitted*, never defaulted to zero).

**Moves** (HOTS -> `hots-connector`, on `kafka_topic_moves`): one Kafka
message per move, `{"v": 1, "container_name": str, "old_host": str | null, "new_host": str, "time": int}`.
`old_host` is `null` for an initial placement (the workload had no prior
host) -- `PlacementMove.source_host` is `None` in that case, and the `docker`
"recreate" strategy currently rejects it explicitly (there's no source
container to clone from; see "Migration semantics" below).

**Schema version.** Both message types carry `"v": 1`
(`hots_connector.transport.kafka.SCHEMA_VERSION`). `_decode_move` treats it
as optional on the way in -- absent (a pre-versioning HOTS build) is
tolerated silently, present-but-unexpected is parsed anyway with a WARNING
log rather than a hard failure, since rejecting outright would turn a minor
version bump into a production outage instead of a visible signal. There is
still no shared package -- the contract is enforced by the fixtures + both
repos' test suites staying in sync, not by a schema registry. If you bump
`SCHEMA_VERSION` or change either message shape, update the fixture in both
repos (byte-identical) in the same change, and update `hots`'s
`placement._move()` (the single helper that stamps `v` on the HOTS side) to
match.

## Settings

All settings are read via `pydantic-settings` from environment variables
prefixed `HOTSCONNECTOR_` (or a `.env` file); see `.env_default` for a
runnable template with every variable and its default.

The CLI reads `.env` from the working directory. Set
`HOTSCONNECTOR_ENV_FILE` to read another file instead, or to an empty value
to read none (environment variables only).

| Setting | Default | Notes |
| --- | --- | --- |
| `log_level` | `INFO` | |
| `observation_backend` | `prometheus_docker` | or `prometheus_kubernetes` |
| `transport_backend` | `kafka` | only backend today |
| `action_backend` | `docker` | or `kubernetes` |
| `prometheus_url` | `http://localhost:9090` | |
| `prom_workload_label`, `prom_host_label`, `prom_query_mem_mb`, `prom_query_cpu` | unset | Unset uses the selected profile's default labels/queries (docker: `name`/`container_label_machine_id`; kubernetes: `pod`/`node`); set any of them to override just that one. |
| `kafka_bootstrap_servers` | `localhost:9092` | |
| `kafka_topic_data`, `kafka_topic_moves` | `hots_data`, `hots_moves` | must match HOTS's `connector.parameters.topic_data`/`topic_moves` |
| `kafka_topic_moves_dlq` | unset | if set, malformed moves are republished here (raw bytes) instead of only being logged |
| `kafka_group_id` | `hots_connector` | |
| `moves_batch_size` | `50` | max moves drained per `consume_moves` call |
| `publish_interval_seconds` | `10` | tick interval |
| `docker_hosts_json` | `{}` | `{"hostId": "tcp://host:2375", ...}`; unknown/empty host ids fall back to `docker_default_host` |
| `docker_default_host` | `unix:///var/run/docker.sock` | |
| `docker_tls_verify`, `docker_tls_ca_cert`, `docker_tls_client_cert`, `docker_tls_client_key` | off / unset | required together if TLS is enabled |
| `migration_strategy` | `recreate` | only strategy today |
| `migration_stop_timeout_seconds` | `10` | grace period for stopping the source container |
| `migration_remove_source` | `true` | remove the source container after a successful migration |
| `migration_keep_name` | `true` | reuse the source container's name on the target |
| `migration_dry_run` | `false` | log the migration that would happen without touching Docker |
| `migration_ready_timeout_seconds`, `migration_ready_poll_seconds` | `30`, `1.0` | how long / how often to poll the target container for `running` before declaring the move failed |
| `k8s_kubeconfig_path`, `k8s_context` | unset | unset uses in-cluster config |
| `k8s_namespace` | `default` | |
| `k8s_workload_identity` | `pod` | or `controller`; `controller` mode is ambiguous for `replicas > 1` -- see "Kubernetes action backend" below |
| `k8s_node_label_key` | `kubernetes.io/hostname` | label patched into the Deployment's `nodeSelector` |
| `k8s_ready_timeout_seconds`, `k8s_poll_interval_seconds` | `120`, `2.0` | how long / how often to poll for a Ready pod on the target node |

## Migration semantics (Docker `recreate` strategy)

`DockerActionBackend` / `DockerMigrator` implements one strategy,
`recreate`: clone the source container's image, command, env, labels,
hostname, user, working dir, published ports, bind mounts and restart
policy onto the target host, start it, wait for it to report `running`, then
stop (and optionally remove) the source. Read this before using it against
anything beyond a lab/benchmark setup:

- **Not migrated**: everything in `HostConfig` besides ports/binds/restart
  policy -- resource limits (CPU/memory limits, not just requests), network
  mode, capabilities, security options, `--link`s, custom DNS, sysctls,
  device mappings. A container relying on any of these will behave
  differently after migration.
- **Duplicate window, no rollback**: the target is created and started
  *before* the source is stopped, to minimize downtime for idempotent
  stateless services. For anything consuming a queue or holding a
  lock/singleton resource, this means both instances can run
  simultaneously and may double-process work. If creating/starting the
  target fails, the source is left untouched (safe). If the target starts
  fine but stopping the source subsequently fails, **both instances are left
  running** -- this is logged as `DUPLICATE_INSTANCE` at ERROR level and
  requires manual cleanup; there is no automatic reconciliation.
  `enable.auto.commit` on the Kafka side is disabled and the transport
  commits only after `apply_moves` returns, so a crash between consuming a
  move and applying it redelivers the move rather than losing it -- but a
  redelivered move after a *partial* apply (target up, source not yet
  stopped) will attempt to create the target a second time and fail loudly
  rather than silently duplicating further.
- **Readiness is `running`, not "actually healthy"**: the target is polled
  until Docker reports its status as `running` (or a terminal bad status,
  which fails fast). This does not inspect an application-level health
  check; a container that starts and then errors out on its first real
  request past the poll window will still be reported as a successful
  migration.
- **Use `migration_dry_run=true`** to validate a whole HOTS run's worth of
  moves (what would be created, with what ports/volumes/restart policy, on
  which hosts) without touching Docker at all.

## Kubernetes action backend (v0)

`KubernetesActionBackend` patches the workload's owning Deployment with a
`nodeSelector` pinning `k8s_node_label_key` to the move's target host, then
evicts the specific pod so its ReplicaSet recreates it -- landing on the
target node because of the nodeSelector. Kubernetes does the actual
scheduling; the connector only asserts *where* the next pod must land. No
image copying or config cloning, unlike the Docker backend.

**Identity mapping** (`k8s_workload_identity`) -- this is the likeliest
source of integration bugs, so it's explicit and documented rather than
guessed at runtime:

- `pod` (default): `PlacementMove.workload_id` is the pod name that was
  observed, matching the raw telemetry identity (`prometheus_kubernetes`'s
  default `pod` label). Resolved to its owning Deployment by walking
  `ownerReferences` (Pod -> ReplicaSet -> Deployment). If the pod was
  already replaced by something else between observation and the move
  arriving, this fails with a clear "pod not found" error rather than
  silently doing nothing.
- `controller`: `workload_id` names the Deployment directly. The backend
  then picks the **first** of the Deployment's pods not already on the
  target host to evict (`_find_pod_not_on_host`) -- correct for the common
  single-replica benchmark case. For `replicas > 1` this is genuinely
  ambiguous: if HOTS issues two moves for two different replicas of the same
  Deployment before the first eviction lands, both could resolve to the same
  pod. Not fixed in v0; safe today only for single-replica workloads, or
  workloads where you don't care which replica moves.

**Not implemented in v0**: per-pod precision via Koordinator's
`PodMigrationJob` CRD or the descheduler eviction API. `validate()` only checks that the
Kubernetes API is reachable and the configured namespace is listable --
it does not check that a specific workload/Deployment exists.

### nodeSelector is sticky

Every move patches the target Deployment's `nodeSelector` and never reverts
it, so it persists after HOTS stops running and constrains where Kubernetes
will schedule that Deployment's pods in the future -- including for a
*different* optimizer's run against the same cluster later (e.g. baseline
after optimized, in a before/after comparison). Two ways to deal with it:

1. **Call `POST /kubernetes/reset-node-selectors`** (or
   `KubernetesActionBackend.restore_node_selectors()` directly) once the run
   is done. The backend snapshots each Deployment's original `nodeSelector`
   the first time it patches it and this puts it back. By default the
   snapshot is in-memory only (per connector process) and is lost if the
   process restarts, so it only covers Deployments patched within that
   process's lifetime -- which matches running the connector as one
   long-lived process per benchmark run (`start`/`stop`). Set
   `HOTSCONNECTOR_K8S_NODE_SELECTOR_SNAPSHOT_PATH` to a writable file path to
   also persist the snapshot to disk, so a crashed/restarted process can
   still restore Deployments it patched before dying. Neither mode is a
   durable audit trail; see the platform's provenance capture for that.
   Restoring the selector only affects where *future* recreations land; it
   does not evict pods back to their pre-run nodes.
2. **Destroy and recreate the cluster between runs** (e.g. a fresh Kind
   cluster per run), which the benchmark platform already does -- in that
   case the sticky selector never outlives the run and step 1 is optional
   belt-and-suspenders.

Either way, a HOTS run mutates Deployment specs, so a benchmark's provenance
record should note that this happened, even after it's cleaned up.

## Testing

```bash
pytest
ruff check hots_connector tests
mypy hots_connector
```

- `tests/unit/` -- pure unit tests needing no infrastructure: message codec
  edge cases, Docker container-config cloning against representative
  `docker inspect` payloads, Prometheus vector parsing against representative
  query responses, Kafka transport consume/commit behavior against a fake
  consumer/producer, and `ConnectorService`'s commit-after-apply semantics.
- `tests/contracts/` -- golden-fixture-driven tests against the shared HOTS
  message contract (see above).
- `tests/integration/` -- exercises the FastAPI app directly: `/health` and
  the `/kubernetes/reset-node-selectors` endpoint's three outcomes.

CI (`.github/workflows/ci.yml`) runs ruff, mypy and pytest on Python 3.10 to
3.13, builds and checks the package, and compares the contract fixtures with
HOTS's copy.

## License

MIT, see `LICENSE.txt`.
