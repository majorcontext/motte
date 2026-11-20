package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/majorcontext/motte/meta"
)

const userTokenColumns = `id, principal, scope, repos, created_at, expires_at, revoked_at`

func (s *Store) CreateUserToken(ctx context.Context, hash string, t meta.UserToken) error {
	repos, err := json.Marshal(append([]string{}, t.Repos...))
	if err != nil {
		return fmt.Errorf("sqlite: create user token: %w", err)
	}
	var expires any
	if !t.ExpiresAt.IsZero() {
		expires = t.ExpiresAt.UnixMicro()
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO user_tokens (id, hash, principal, scope, repos, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		t.ID, hash, t.Principal, t.Scope, string(repos), micros(), expires)
	if uniqueViolation(err) {
		return meta.ErrExists
	}
	if err != nil {
		return fmt.Errorf("sqlite: create user token: %w", err)
	}
	return nil
}

type rowScanner interface{ Scan(...any) error }

func scanUserToken(row rowScanner) (meta.UserToken, error) {
	var t meta.UserToken
	var repos string
	var created int64
	var expires, revoked sql.NullInt64
	if err := row.Scan(&t.ID, &t.Principal, &t.Scope, &repos, &created, &expires, &revoked); err != nil {
		return meta.UserToken{}, err
	}
	if err := json.Unmarshal([]byte(repos), &t.Repos); err != nil {
		return meta.UserToken{}, err
	}
	t.CreatedAt = time.UnixMicro(created).UTC()
	if expires.Valid {
		t.ExpiresAt = time.UnixMicro(expires.Int64).UTC()
	}
	if revoked.Valid {
		t.RevokedAt = time.UnixMicro(revoked.Int64).UTC()
	}
	return t, nil
}

func (s *Store) UserTokenByHash(ctx context.Context, hash string) (meta.UserToken, error) {
	t, err := scanUserToken(s.db.QueryRowContext(ctx, `SELECT `+userTokenColumns+` FROM user_tokens WHERE hash = ?`, hash))
	if errors.Is(err, sql.ErrNoRows) {
		return meta.UserToken{}, meta.ErrNotFound
	}
	if err != nil {
		return meta.UserToken{}, fmt.Errorf("sqlite: read user token: %w", err)
	}
	return t, nil
}

func (s *Store) UserTokens(ctx context.Context) ([]meta.UserToken, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+userTokenColumns+` FROM user_tokens ORDER BY created_at DESC, id DESC`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list user tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []meta.UserToken
	for rows.Next() {
		t, err := scanUserToken(rows)
		if err != nil {
			return nil, fmt.Errorf("sqlite: list user tokens: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) RevokeUserToken(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE user_tokens SET revoked_at = COALESCE(revoked_at, ?) WHERE id = ?`, micros(), id)
	if err != nil {
		return fmt.Errorf("sqlite: revoke user token: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return meta.ErrNotFound
	}
	return nil
}
