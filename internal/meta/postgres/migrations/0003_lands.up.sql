CREATE TABLE land_queue (
    repo text NOT NULL,
    id text NOT NULL,
    seq bigint GENERATED ALWAYS AS IDENTITY,
    ref text NOT NULL,
    base text NOT NULL,
    head text NOT NULL,
    kind text NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'claimed', 'done')),
    owner text NOT NULL DEFAULT '',
    result text NOT NULL DEFAULT '',
    generation bigint,
    enqueued_at timestamp with time zone NOT NULL DEFAULT now(),
    done_at timestamp with time zone,
    PRIMARY KEY (repo, id)
);

CREATE INDEX land_queue_open ON land_queue (repo, ref, seq) WHERE state <> 'done';
CREATE INDEX land_queue_done_at ON land_queue (done_at) WHERE state = 'done';

CREATE TABLE land_leases (
    repo text NOT NULL,
    ref text NOT NULL,
    owner text NOT NULL,
    lease_until timestamp with time zone NOT NULL,
    PRIMARY KEY (repo, ref)
);

CREATE TABLE land_heads (
    repo text NOT NULL,
    ref text NOT NULL,
    n bigint NOT NULL,
    PRIMARY KEY (repo, ref)
);

CREATE TABLE land_log (
    repo text NOT NULL,
    ref text NOT NULL,
    n bigint NOT NULL,
    old_tip text NOT NULL,
    new_tip text NOT NULL,
    paths jsonb,
    PRIMARY KEY (repo, ref, n)
);

CREATE INDEX land_log_old ON land_log (repo, ref, old_tip);
CREATE INDEX land_log_new ON land_log (repo, ref, new_tip);
