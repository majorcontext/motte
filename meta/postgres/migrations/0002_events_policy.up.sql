CREATE TABLE event_partitions (
    partition integer PRIMARY KEY CHECK (partition >= 0 AND partition < 64),
    seq bigint NOT NULL,
    floor bigint NOT NULL
);

INSERT INTO event_partitions (partition, seq, floor) SELECT p, 0, 0 FROM generate_series(0, 63) AS p;

CREATE TABLE events (
    partition integer NOT NULL REFERENCES event_partitions (partition),
    seq bigint NOT NULL CHECK (seq > 0),
    repo text NOT NULL,
    ref text NOT NULL,
    old_commit text NOT NULL,
    new_commit text NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    kind text NOT NULL,
    at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (partition, seq)
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
    created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE subscription_cursors (
    sub_id text NOT NULL REFERENCES subscriptions (id) ON DELETE CASCADE,
    partition integer NOT NULL,
    cursor bigint NOT NULL,
    lease_owner text NOT NULL DEFAULT '',
    lease_until timestamp with time zone NOT NULL DEFAULT 'epoch',
    PRIMARY KEY (sub_id, partition)
);
