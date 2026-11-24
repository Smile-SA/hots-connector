# Contract Tests

These pin the **message contract between HOTS and `hots-connector`**, so a
change to either side's message handling fails here rather than silently at
integration time.

`fixtures/` is copied **byte-identical** from HOTS's `tests/contracts/fixtures/`
(see that repo's `tests/contracts/README.md` for the canonical description).
Whichever repo changes a message shape has to change the fixture, which
breaks the other repo's suite on the next sync -- that's the point.

| File | Direction | Topic |
| --- | --- | --- |
| `telemetry_batch.json` | `hots-connector` -> HOTS | `HOTSCONNECTOR_KAFKA_TOPIC_DATA` / `connector.parameters.topic_data` |
| `moves_batch.json` | HOTS -> `hots-connector` | `HOTSCONNECTOR_KAFKA_TOPIC_MOVES` / `connector.parameters.topic_moves` |

`test_message_contract.py` is this repo's (consumer/producer) verification:

- the fixtures are self-consistent and carry the documented keys (duplicated
  from the HOTS-side check, so a drifted fixture fails here too);
- `_encode_batch` produces exactly `telemetry_batch.json`'s messages from an
  equivalent `WorkloadBatch` -- **note the wire shape is `{"v": 1, "containers": [...]}`
  only**, with no top-level `timestamp` or `file` key;
- `_decode_move` parses every message in `moves_batch.json` into the correct
  `PlacementMove`, including a null `old_host` (initial placement) and the
  `v` schema-version key.

Pure unit-level edge cases for the same codec functions (malformed JSON,
missing keys, unexpected extra keys, missing/mismatched `v`) live in
`tests/unit/test_message_codec.py` instead, decoupled from the fixtures.

## Schema version

Both message types carry `"v": 1` (`hots_connector.transport.kafka.SCHEMA_VERSION`).
It's stamped by `_encode_batch` here and by `placement._move()` on the HOTS
side -- the one helper every HOTS move producer goes through, per that
repo's own contract notes. `_decode_move` treats `v` as optional-but-checked:
absent is tolerated (an older, pre-versioning sender), present-but-different
is parsed anyway with a WARNING log. Bumping it is a coordinated, two-repo
change: update `SCHEMA_VERSION`/`_move()`, both copies of the fixtures
(byte-identical), and both repos' shape assertions in the same change.

## Keeping the two repos in sync

There's no shared package yet (a small shared `hots-messages` package or a
versioned JSON Schema is the longer-term option). Until then: if you change
either fixture file, copy it to the other repo and update both suites in the
same change.

CI checks this: the `contract-sync` job in `.github/workflows/ci.yml`
downloads HOTS's fixtures at the tag named by `HOTS_CONTRACT_REF` and
compares them byte for byte with `fixtures/`. Bump that tag when a new HOTS
release changes the contract.
