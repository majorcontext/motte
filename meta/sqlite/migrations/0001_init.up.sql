CREATE TABLE repos (
    path TEXT PRIMARY KEY,
    name TEXT NOT NULL CHECK (trim(name) <> ''),
    owner TEXT NOT NULL CHECK (trim(owner) <> ''),
    created_at INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE packs (
    repo TEXT NOT NULL,
    pack_id TEXT NOT NULL,
    bytes INTEGER NOT NULL CHECK (bytes >= 0),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (repo, pack_id)
) WITHOUT ROWID;

CREATE TABLE counters (
    name TEXT PRIMARY KEY,
    value INTEGER NOT NULL
) WITHOUT ROWID;

INSERT INTO counters (name, value) VALUES ('ref_generation', 0);

CREATE TABLE refs (
    repo TEXT NOT NULL,
    name TEXT NOT NULL,
    commit_sha TEXT NOT NULL CHECK (length(commit_sha) = 40 AND commit_sha NOT GLOB '*[^0-9a-f]*'),
    generation INTEGER NOT NULL CHECK (generation > 0),
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (repo, name)
) WITHOUT ROWID;

CREATE TABLE workspaces (
    id TEXT PRIMARY KEY,
    repo TEXT NOT NULL,
    owner TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    op TEXT NOT NULL DEFAULT '' CHECK (op = '' OR (length(op) BETWEEN 16 AND 128 AND op NOT GLOB '*[^0-9a-f]*')),
    snapshot TEXT NOT NULL DEFAULT '' CHECK (snapshot = '' OR (length(snapshot) IN (40, 64) AND snapshot NOT GLOB '*[^0-9a-f]*')),
    report_generation INTEGER CHECK (report_generation IS NULL OR report_generation > 0),
    created_at INTEGER NOT NULL,
    deleted_at INTEGER
) WITHOUT ROWID;

CREATE TABLE op_heads (
    workspace_id TEXT PRIMARY KEY REFERENCES workspaces (id),
    epoch INTEGER NOT NULL,
    generation INTEGER NOT NULL CHECK (generation > 0),
    heads TEXT NOT NULL CHECK (json_array_length(heads) > 0),
    updated_at INTEGER NOT NULL
) WITHOUT ROWID;
