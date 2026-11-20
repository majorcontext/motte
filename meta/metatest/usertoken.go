package metatest

import (
	"reflect"
	"testing"
	"time"

	"github.com/majorcontext/motte/meta"
)

var userTokenCases = []struct {
	name string
	fn   func(*testing.T, meta.Store)
}{
	{"a user token is stored by hash and never twice", userTokenStored},
	{"revoke keeps the first time and refuses an unknown id", userTokenRevoke},
	{"user tokens list newest first", userTokensList},
}

func userTokenStored(t *testing.T, s meta.Store) {
	exp := time.Now().Add(time.Hour).UTC().Truncate(time.Microsecond)
	want := meta.UserToken{ID: "ut_1", Principal: "dev@example.test", Scope: "write", Repos: []string{"native:repo_A", "native:repo_B"}, ExpiresAt: exp}
	ok(t, s.CreateUserToken(ctx, "hash1", want))
	got, err := s.UserTokenByHash(ctx, "hash1")
	ok(t, err)
	if got.CreatedAt.IsZero() || !got.RevokedAt.IsZero() || !got.ExpiresAt.Equal(exp) || !reflect.DeepEqual(got.Repos, want.Repos) || got.ID != want.ID || got.Principal != want.Principal || got.Scope != want.Scope {
		t.Errorf("read = %+v, want %+v with a creation time", got, want)
	}
	is(t, s.CreateUserToken(ctx, "hash1", meta.UserToken{ID: "ut_2", Principal: "a@b", Scope: "read"}), meta.ErrExists)
	is(t, s.CreateUserToken(ctx, "hash2", meta.UserToken{ID: "ut_1", Principal: "a@b", Scope: "read"}), meta.ErrExists)
	_, err = s.UserTokenByHash(ctx, "missing")
	is(t, err, meta.ErrNotFound)
}

func userTokenRevoke(t *testing.T, s meta.Store) {
	ok(t, s.CreateUserToken(ctx, "h", meta.UserToken{ID: "ut_1", Principal: "a@b", Scope: "admin"}))
	ok(t, s.RevokeUserToken(ctx, "ut_1"))
	first, err := s.UserTokenByHash(ctx, "h")
	ok(t, err)
	ok(t, s.RevokeUserToken(ctx, "ut_1"))
	again, _ := s.UserTokenByHash(ctx, "h")
	if first.RevokedAt.IsZero() || !again.RevokedAt.Equal(first.RevokedAt) {
		t.Errorf("revoked at %v then %v, want one fixed time", first.RevokedAt, again.RevokedAt)
	}
	is(t, s.RevokeUserToken(ctx, "ut_none"), meta.ErrNotFound)
}

func userTokensList(t *testing.T, s meta.Store) {
	for _, id := range []string{"ut_a", "ut_b", "ut_c"} {
		ok(t, s.CreateUserToken(ctx, "h-"+id, meta.UserToken{ID: id, Principal: "a@b", Scope: "read", Repos: []string{"native:repo_A"}}))
		time.Sleep(2 * time.Millisecond) //lint:allow wallclock creation times differ by the backend clock
	}
	got, err := s.UserTokens(ctx)
	ok(t, err)
	var ids []string
	for _, tok := range got {
		ids = append(ids, tok.ID)
	}
	if !reflect.DeepEqual(ids, []string{"ut_c", "ut_b", "ut_a"}) {
		t.Errorf("order = %v, want newest first", ids)
	}
}
