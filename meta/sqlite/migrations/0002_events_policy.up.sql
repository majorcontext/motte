INSERT INTO counters (name, value) VALUES ('events_floor', 0);

CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    repo TEXT NOT NULL,
    ref TEXT NOT NULL,
    old_commit TEXT NOT NULL,
    new_commit TEXT NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    kind TEXT NOT NULL,
    at INTEGER NOT NULL
);

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
    cursor INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_until INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;
