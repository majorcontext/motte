package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/majorcontext/motte/meta"
)

const userTokenColumns = `id, principal, scope, repos, created_at, expires_at, revoked_at`

func (s *Store) CreateUserToken(ctx context.Context, hash string, t meta.UserToken) error {
	repos := t.Repos
	if repos == nil {
		repos = []string{}
	}
	var expires *time.Time
	if !t.ExpiresAt.IsZero() {
		expires = &t.ExpiresAt
	}
	_, err := s.Pool.Exec(ctx, `INSERT INTO user_tokens (id, hash, principal, scope, repos, expires_at) VALUES ($1, $2, $3, $4, $5, $6)`,
		t.ID, hash, t.Principal, t.Scope, repos, expires)
	if uniqueViolation(err) {
		return meta.ErrExists
	}
	if err != nil {
		return fmt.Errorf("postgres: create user token: %w", err)
	}
	return nil
}

func scanUserToken(row pgx.Row) (meta.UserToken, error) {
	var t meta.UserToken
	var expires, revoked *time.Time
	if err := row.Scan(&t.ID, &t.Principal, &t.Scope, &t.Repos, &t.CreatedAt, &expires, &revoked); err != nil {
		return meta.UserToken{}, err
	}
	t.CreatedAt = t.CreatedAt.UTC()
	if expires != nil {
		t.ExpiresAt = expires.UTC()
	}
	if revoked != nil {
		t.RevokedAt = revoked.UTC()
	}
	return t, nil
}

func (s *Store) UserTokenByHash(ctx context.Context, hash string) (meta.UserToken, error) {
	t, err := scanUserToken(s.Pool.QueryRow(ctx, `SELECT `+userTokenColumns+` FROM user_tokens WHERE hash = $1`, hash))
	if errors.Is(err, pgx.ErrNoRows) {
		return meta.UserToken{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.UserToken{}, fmt.Errorf("postgres: read user token: %w", err)
	}
	return t, nil
}

func (s *Store) UserTokens(ctx context.Context) ([]meta.UserToken, error) {
	rows, err := s.Pool.Query(ctx, `SELECT `+userTokenColumns+` FROM user_tokens ORDER BY created_at DESC, id DESC`)
	if err != nil {
		return nil, fmt.Errorf("postgres: list user tokens: %w", err)
	}
	defer rows.Close()
	var out []meta.UserToken
	for rows.Next() {
		t, err := scanUserToken(rows)
		if err != nil {
			return nil, fmt.Errorf("postgres: list user tokens: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) RevokeUserToken(ctx context.Context, id string) error {
	tag, err := s.Pool.Exec(ctx, `UPDATE user_tokens SET revoked_at = COALESCE(revoked_at, now()) WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("postgres: revoke user token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return meta.ErrNotFound
	}
	return nil
}
