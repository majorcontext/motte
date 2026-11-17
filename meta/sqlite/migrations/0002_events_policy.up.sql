CREATE TABLE event_partitions (
    partition INTEGER PRIMARY KEY CHECK (partition >= 0 AND partition < 64),
    seq INTEGER NOT NULL,
    floor INTEGER NOT NULL
);

WITH RECURSIVE n(p) AS (SELECT 0 UNION ALL SELECT p + 1 FROM n WHERE p < 63)
INSERT INTO event_partitions (partition, seq, floor) SELECT p, 0, 0 FROM n;

CREATE TABLE events (
    partition INTEGER NOT NULL,
    seq INTEGER NOT NULL CHECK (seq > 0),
    repo TEXT NOT NULL,
    ref TEXT NOT NULL,
    old_commit TEXT NOT NULL,
    new_commit TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    kind TEXT NOT NULL,
    at INTEGER NOT NULL,
    PRIMARY KEY (partition, seq)
) WITHOUT ROWID;

CREATE INDEX events_at ON events (at);

CREATE TABLE protected_refs (
    repo TEXT PRIMARY KEY,
    patterns TEXT NOT NULL CHECK (json_valid(patterns))
) WITHOUT ROWID;

CREATE TABLE subscriptions (
    id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    repo TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE subscription_cursors (
    sub_id TEXT NOT NULL REFERENCES subscriptions (id) ON DELETE CASCADE,
    partition INTEGER NOT NULL,
    cursor INTEGER NOT NULL,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_until INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (sub_id, partition)
) WITHOUT ROWID;
