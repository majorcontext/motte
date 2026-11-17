CREATE TABLE counters (
    name text PRIMARY KEY,
    value bigint NOT NULL
);

INSERT INTO counters (name, value) VALUES ('event_seq', 0), ('events_floor', 0);

CREATE TABLE events (
    seq bigint PRIMARY KEY,
    repo text NOT NULL,
    ref text NOT NULL,
    old_commit text NOT NULL,
    new_commit text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    kind text NOT NULL,
    at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX events_at ON events (at);

CREATE TABLE protected_refs (
    repo text PRIMARY KEY,
    patterns text[] NOT NULL
);

CREATE TABLE subscriptions (
    id text PRIMARY KEY,
    url text NOT NULL,
    secret text NOT NULL,
    repo text NOT NULL DEFAULT '',
    cursor bigint NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    lease_owner text NOT NULL DEFAULT '',
    lease_until timestamp with time zone NOT NULL DEFAULT 'epoch'
);
