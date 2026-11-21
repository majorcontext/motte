CREATE TABLE pack_manifests (
    repo text PRIMARY KEY,
    generation bigint NOT NULL,
    body bytea NOT NULL,
    updated_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE pack_retired (
    repo text NOT NULL,
    key text NOT NULL,
    retired_at timestamp with time zone NOT NULL,
    deleting_at timestamp with time zone,
    PRIMARY KEY (repo, key)
);
