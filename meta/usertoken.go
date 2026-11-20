package meta

import (
	"context"
	"time"
)

// UserToken is the record of a per-user token. The store keeps the hash of the token and never the token. ExpiresAt
// and RevokedAt are zero when the token has no expiry or is not revoked. CreatedAt is set by the backend.
type UserToken struct {
	ID, Principal, Scope string
	Repos                []string
	CreatedAt, ExpiresAt time.Time
	RevokedAt            time.Time
}

// UserTokenStore keeps per-user tokens. hash is the lowercase hex SHA-256 of the token text.
type UserTokenStore interface {
	// CreateUserToken stores t under hash. ErrExists when t.ID or hash exists.
	CreateUserToken(ctx context.Context, hash string, t UserToken) error
	// UserTokenByHash returns the record under hash, revoked or not, or ErrNotFound.
	UserTokenByHash(ctx context.Context, hash string) (UserToken, error)
	// UserTokens lists every record, newest first, ties by ID descending.
	UserTokens(ctx context.Context) ([]UserToken, error)
	// RevokeUserToken sets RevokedAt of id once. A repeat keeps the first time and is not an error. ErrNotFound
	// for an unknown id.
	RevokeUserToken(ctx context.Context, id string) error
}
