-- 001: alerts, cases and the log bundles that can be attached to them.
-- Everything lives in the "cases" schema: this service owns these tables and
-- never touches the telemetry processor's, and vice versa.

CREATE SCHEMA IF NOT EXISTS cases;

-- Every alert ever received, once. The primary key is the first layer of
-- deduplication: an agent retry, a Kafka redelivery or a full topic replay
-- inserts nothing the second time.
CREATE TABLE IF NOT EXISTS cases.alerts (
    alert_id     TEXT PRIMARY KEY,
    device_id    TEXT NOT NULL,
    site_id      TEXT NOT NULL DEFAULT '',
    code         TEXT NOT NULL,
    severity     TEXT NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    message      TEXT NOT NULL DEFAULT '',
    observed_at  TIMESTAMPTZ NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL,
    bundle_id    TEXT,
    rule_id      TEXT,
    -- What the rules decided: no_rule, below_threshold, opened or attached.
    outcome      TEXT NOT NULL,
    case_id      BIGINT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Threshold windows count a device's alerts of one code; storm windows count
-- a site's devices. Both look back from the alert's observed time.
CREATE INDEX IF NOT EXISTS alerts_device_code_observed ON cases.alerts (device_id, code, observed_at);
CREATE INDEX IF NOT EXISTS alerts_site_code_observed ON cases.alerts (site_id, code, observed_at);
CREATE INDEX IF NOT EXISTS alerts_case ON cases.alerts (case_id) WHERE case_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS alerts_bundle ON cases.alerts (bundle_id) WHERE bundle_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS cases.cases (
    case_id        BIGSERIAL PRIMARY KEY,
    scope          TEXT NOT NULL CHECK (scope IN ('device', 'site')),
    scope_key      TEXT NOT NULL,      -- the device ID, or the site ID for a site case
    site_id        TEXT NOT NULL DEFAULT '',
    device_id      TEXT,               -- NULL for a site case
    rule_id        TEXT NOT NULL,
    title          TEXT NOT NULL,
    severity       TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    alert_count    INT NOT NULL DEFAULT 0,
    parent_case_id BIGINT REFERENCES cases.cases (case_id),
    opened_at      TIMESTAMPTZ NOT NULL,
    last_alert_at  TIMESTAMPTZ NOT NULL,
    resolved_at    TIMESTAMPTZ,
    resolution     TEXT NOT NULL DEFAULT ''
);

-- The second layer of deduplication: at most one open case per subject and
-- rule. The database enforces it, so two consumers racing on the same device
-- or site can't both open one; the loser attaches to the winner's case.
CREATE UNIQUE INDEX IF NOT EXISTS cases_one_open_per_subject
    ON cases.cases (scope, scope_key, rule_id) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS cases_status_opened ON cases.cases (status, opened_at DESC);
CREATE INDEX IF NOT EXISTS cases_parent ON cases.cases (parent_case_id) WHERE parent_case_id IS NOT NULL;

-- This service's own index of uploaded log bundles, fed by bundles.uploaded.
-- A bundle can arrive before or after the alert that names it; a case's logs
-- are whatever its alerts name that has arrived here.
CREATE TABLE IF NOT EXISTS cases.bundles (
    bundle_id    TEXT PRIMARY KEY,
    device_id    TEXT NOT NULL,
    reason       TEXT NOT NULL,
    object_key   TEXT NOT NULL,
    size_bytes   BIGINT NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    uploaded_at  TIMESTAMPTZ NOT NULL,
    indexed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
