CREATE TABLE repos (
    path text PRIMARY KEY,
    name text NOT NULL CHECK (name !~ '^[[:space:]]*$'),
    owner text NOT NULL CHECK (owner !~ '^[[:space:]]*$'),
    created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE TABLE packs (
    repo text NOT NULL,
    pack_id text NOT NULL,
    bytes bigint NOT NULL CHECK (bytes >= 0),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (repo, pack_id)
);

CREATE SEQUENCE ref_generation;

CREATE TABLE refs (
    repo text NOT NULL,
    name text NOT NULL,
    commit_sha text NOT NULL CHECK (commit_sha ~ '^[0-9a-f]{40}$'),
    generation bigint NOT NULL CHECK (generation > 0),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    PRIMARY KEY (repo, name)
);

CREATE TABLE workspaces (
    id text PRIMARY KEY,
    repo text NOT NULL,
    owner text NOT NULL,
    epoch bigint NOT NULL CHECK (epoch > 0),
    op text NOT NULL DEFAULT '' CHECK (op ~ '^([0-9a-f]{16,128})?$'),
    snapshot text NOT NULL DEFAULT '' CHECK (snapshot ~ '^([0-9a-f]{40}|[0-9a-f]{64})?$'),
    report_generation bigint CHECK (report_generation IS NULL OR report_generation > 0),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    deleted_at timestamp with time zone
);

CREATE TABLE op_heads (
    workspace_id text PRIMARY KEY REFERENCES workspaces (id),
    epoch bigint NOT NULL,
    generation bigint NOT NULL CHECK (generation > 0),
    heads text[] NOT NULL CHECK (cardinality(heads) > 0),
    updated_at timestamp with time zone NOT NULL DEFAULT now()
);
