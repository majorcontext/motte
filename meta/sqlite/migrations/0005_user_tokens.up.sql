CREATE TABLE user_tokens (
    id TEXT PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    principal TEXT NOT NULL,
    scope TEXT NOT NULL,
    repos TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER,
    revoked_at INTEGER
) WITHOUT ROWID;
