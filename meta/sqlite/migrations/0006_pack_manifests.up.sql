CREATE TABLE pack_manifests (
    repo TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    body BLOB NOT NULL,
    updated_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE pack_retired (
    repo TEXT NOT NULL,
    key TEXT NOT NULL,
    retired_at INTEGER NOT NULL,
    deleting_at INTEGER,
    PRIMARY KEY (repo, key)
) WITHOUT ROWID;
