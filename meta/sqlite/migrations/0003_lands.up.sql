CREATE TABLE land_queue (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    repo TEXT NOT NULL,
    id TEXT NOT NULL,
    ref TEXT NOT NULL,
    base TEXT NOT NULL,
    head TEXT NOT NULL,
    kind TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'claimed', 'done')),
    owner TEXT NOT NULL DEFAULT '',
    result TEXT NOT NULL DEFAULT '',
    generation INTEGER,
    enqueued_at INTEGER NOT NULL,
    done_at INTEGER,
    UNIQUE (repo, id)
);

CREATE INDEX land_queue_open ON land_queue (repo, ref, seq) WHERE state <> 'done';
CREATE INDEX land_queue_done_at ON land_queue (done_at) WHERE state = 'done';

CREATE TABLE land_leases (
    repo TEXT NOT NULL,
    ref TEXT NOT NULL,
    owner TEXT NOT NULL,
    lease_until INTEGER NOT NULL,
    PRIMARY KEY (repo, ref)
) WITHOUT ROWID;

CREATE TABLE land_heads (
    repo TEXT NOT NULL,
    ref TEXT NOT NULL,
    n INTEGER NOT NULL,
    PRIMARY KEY (repo, ref)
) WITHOUT ROWID;

CREATE TABLE land_log (
    repo TEXT NOT NULL,
    ref TEXT NOT NULL,
    n INTEGER NOT NULL,
    old_tip TEXT NOT NULL,
    new_tip TEXT NOT NULL,
    paths TEXT CHECK (paths IS NULL OR json_valid(paths)),
    PRIMARY KEY (repo, ref, n)
) WITHOUT ROWID;

CREATE INDEX land_log_old ON land_log (repo, ref, old_tip);
CREATE INDEX land_log_new ON land_log (repo, ref, new_tip);
