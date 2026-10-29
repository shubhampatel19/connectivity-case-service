# connectivity-case-service

Turns device alerts into support cases: the cloud half of a support gateway's "auto case creation". It consumes `device.alerts`, decides with a rule set which alerts deserve a case, keeps one fault from becoming many cases, and attaches the log bundles devices collected for their faults.

Part of [Connectivity](https://github.com/shubhampatel19/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

```
device ── POST /v1/alerts ──▶ ingest-api ──▶ device.alerts ──▶ case-service ──▶ Postgres (schema "cases")
device ── log bundle ───────▶ ingest-api ──▶ bundles.uploaded ─┘                    ▲
                                                               support ── /v1/cases ┘
```

## Rules

A rule says when alerts of one code become a case. The defaults ([`internal/cases/default_rules.json`](internal/cases/default_rules.json)):

| Rule | Code | Case when | Case severity |
|---|---|---|---|
| `thermal-critical` | `thermal.critical` | the first alert; 5+ devices at one site within 10 minutes → one site case | critical |
| `psu-rail-unstable` | `power.rail_out_of_range` | 3 alerts from the device within 15 minutes | major |
| `disk-reallocating` | `storage.reallocated_sectors` | the sector count reaches 5 | minor |

An alert whose code has no rule is recorded with outcome `no_rule` and opens nothing. That is also what happens to codes from agents newer than the rules, so a fleet upgrade can't open cases nobody designed.

To change them, mount a file and set `RULES_FILE`. The service refuses to start on a bad file, and lists every problem in it at once. Unknown fields are errors: a misspelled `treshold` that silently did nothing would open cases nobody expected. `GET /v1/rules` shows what's loaded.

Windows (`threshold`, `storm`) are centred on the alert's observed time on the device, not on when it arrived. Alerts that arrive late or out of order, such as a gateway draining its queue after an outage, still count together.

## Deduplication, in three layers

1. **The same alert twice.** An agent retry, a Kafka redelivery or a whole topic replayed from offset 0: `alert_id` is the primary key of `cases.alerts`, so the second insert does nothing and the alert is counted as `duplicate`.
2. **The same fault again.** While a device has an open case for a rule, every new alert for that rule joins it and bumps `alert_count`. A partial unique index (one open case per subject and rule) enforces it in the database, so two consumers racing on the same device can't both open one.
3. **Many devices, one cause.** When enough devices at one site raise the same code within the storm window, the service opens one site case. Open device cases for that rule at the site become its children, and later alerts join it directly. Resolving the site case resolves the children. A Postgres advisory lock per site and rule serializes these decisions; without it, consumers handling different devices in parallel each count too few devices and miss the storm. The test suite proves it: remove the lock and `TestConcurrentStormYieldsOneSiteCase` fails.

Each alert's outcome (`opened`, `attached`, `below_threshold`, `no_rule`) and its case are recorded on the alert row, so "why didn't this alert open a case?" is a query, not a guess.

## Logs

An alert can name the log bundle the device collected for the fault. The bundle may reach the cloud before or after the alert, so the service consumes `bundles.uploaded` itself and keeps its own index; it never reads the telemetry processor's tables. A case's `logs` is `waiting` until one of the bundles its alerts named has arrived, then `attached`. `none` means no alert named a bundle (the device was within its bundle cooldown, or the fault doesn't collect logs).

## API

No authentication: it's an internal API for the support team, reachable only inside the platform network.

| Route | Purpose |
|---|---|
| `GET /v1/cases` | Cases, newest first. `status=open` (default), `resolved` or `all`; `site`, `device`, `rule`; `limit` (1–500, default 50) |
| `GET /v1/cases/{id}` | One case with its alerts, its bundles (and whether each has arrived), and its child cases |
| `POST /v1/cases/{id}/resolve` | `{"resolution": "..."}`. 409 if already resolved. Resolves child cases too |
| `GET /v1/rules` | The rules in effect |
| `GET /healthz`, `GET /readyz` | Liveness; readiness checks Postgres |
| `GET /metrics` | Prometheus metrics, `connectivity_case_` prefix |

## Failure behavior

| Situation | What happens |
|---|---|
| Postgres down | The record is retried in place with backoff; the partition waits. Consumer lag grows, `connectivity_case_retries_total` rises. Skipping ahead would reorder a device's alerts, and thresholds count them. |
| Crash or rebalance mid-poll | Offsets are committed only after the database commit, so records are replayed and deduplicated by layer 1. |
| Malformed or invalid alert | Logged, counted in `alerts_rejected_total`, skipped. Retrying it would block the partition forever. |
| Rules file invalid | The service doesn't start. |
| Gauge refresh failing | Gauges keep their last values; `stats_last_success_timestamp_seconds` stops moving. |

A new consumer group starts from the beginning of the topics, so a case service deployed after devices began alerting still sees every alert. That also makes a full replay a safe drill: reset the group's offsets and no new cases should appear.

## Configuration

| Variable | Default |
|---|---|
| `DATABASE_URL` | required |
| `KAFKA_BROKERS` | `localhost:29092` |
| `CONSUMER_GROUP` | `case-service` |
| `HTTP_ADDR` | `:8085` (API, health and metrics) |
| `RULES_FILE` | unset: built-in defaults |
| `MAX_POLL_RECORDS` | `200` |
| `STATS_INTERVAL` | `15s` (how often the open-case gauges refresh) |

## Development

```bash
go test ./...   # unit tests; the Postgres tests skip themselves

# the integration tests (they drop and recreate the "cases" schema):
docker run -d --name cases-pg -e POSTGRES_PASSWORD=pg -p 5433:5432 postgres:16-alpine
CASES_TEST_DATABASE_URL='postgres://postgres:pg@localhost:5433/postgres?sslmode=disable' go test -race ./...
```

CI runs them against a Postgres service container and fails if any were skipped.
