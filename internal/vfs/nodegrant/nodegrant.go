// Package nodegrant mints the node grants of the CSI plugin.
package nodegrant

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/volume"
)

// TTL is the life of a node grant. The store keeps a rotated key for as long.
const TTL = time.Hour

// Minter signs the node grant of a workspace volume.
type Minter struct {
	signer grant.Signer
	now    func() time.Time
}

// Load reads a key file: JSON {"kid","key"}, where key is the standard base64 of an Ed25519 private key or of its
// 32-byte seed.
func Load(path string) (*Minter, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var k struct{ Kid, Key string }
	if err := json.Unmarshal(raw, &k); err != nil || k.Kid == "" {
		return nil, errors.New("grant key file: want JSON with kid and key")
	}
	b, err := base64.StdEncoding.DecodeString(k.Key)
	if err != nil {
		return nil, fmt.Errorf("grant key file: key: %w", err)
	}
	switch len(b) {
	case ed25519.SeedSize:
		b = ed25519.NewKeyFromSeed(b)
	case ed25519.PrivateKeySize:
	default:
		return nil, errors.New("grant key file: key is not an Ed25519 private key or seed")
	}
	return &Minter{signer: grant.Signer{Kid: k.Kid, Key: b}, now: time.Now}, nil
}

// Mint returns a node grant for the workspace, epoch and repository of pub. It fails when the claims are incomplete.
func (m *Minter) Mint(pub volume.Publication) (string, error) {
	return m.signer.Mint(grant.Claims{Scope: grant.ScopeNode, WS: pub.Workspace, Epoch: pub.Epoch, Repo: pub.Repo, Exp: m.now().Add(TTL).Unix()})
}
