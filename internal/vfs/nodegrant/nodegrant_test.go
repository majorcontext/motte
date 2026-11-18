package nodegrant

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/volume"
)

func TestLoadAndMint(t *testing.T) {
	t.Parallel()
	pub, priv, _ := ed25519.GenerateKey(nil)
	b64 := base64.StdEncoding.EncodeToString
	write := func(doc string) string {
		p := filepath.Join(t.TempDir(), "key")
		if err := os.WriteFile(p, []byte(doc), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	for name, doc := range map[string]string{
		"private key": fmt.Sprintf(`{"kid":"k1","key":%q}`, b64(priv)),
		"seed":        fmt.Sprintf(`{"kid":"k1","key":%q}`, b64(priv.Seed())),
	} {
		m, err := Load(write(doc))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		now := time.Unix(1_700_000_000, 0)
		m.now = func() time.Time { return now }
		tok, err := m.Mint(volume.Publication{Workspace: "w", Epoch: 2, Repo: "gh:o/r"})
		if err != nil {
			t.Fatal(err)
		}
		c, err := grant.NewVerifier(grant.Key{Kid: "k1", Public: pub, Scopes: []grant.Scope{grant.ScopeNode}}).Verify(tok, now)
		if err != nil || c.WS != "w" || c.Epoch != 2 || c.Repo != "gh:o/r" || c.Exp != now.Add(time.Hour).Unix() {
			t.Errorf("%s: claims %+v %v, want the workspace, epoch and repo and one hour", name, c, err)
		}
		if _, err := m.Mint(volume.Publication{Workspace: "w", Repo: "gh:o/r"}); err == nil {
			t.Errorf("%s: a grant of epoch 0 minted; the caller would send an empty bearer", name)
		}
	}
	for name, doc := range map[string]string{"no kid": `{"key":"AA=="}`, "short key": `{"kid":"k","key":"AAAA"}`, "not base64": `{"kid":"k","key":"!"}`} {
		if _, err := Load(write(doc)); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}
