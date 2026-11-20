CREATE TABLE user_tokens (
    id text PRIMARY KEY,
    hash text NOT NULL UNIQUE,
    principal text NOT NULL,
    scope text NOT NULL,
    repos text[] NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    expires_at timestamp with time zone,
    revoked_at timestamp with time zone
);
