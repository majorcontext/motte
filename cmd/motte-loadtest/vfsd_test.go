package main

import (
	"bytes"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/testserver"
)

func TestRawTreeRebuildsTheBytesThatNameAnEditedTree(t *testing.T) {
	t.Parallel()
	blob := bytes.Repeat([]byte{0xab}, 20)
	dir := bytes.Repeat([]byte{0xcd}, 20)
	ents := []api.TreeEntry{
		{Name: base64.StdEncoding.EncodeToString([]byte("a.go")), Mode: "100644", SHA: hex.EncodeToString(blob)},
		{Name: base64.StdEncoding.EncodeToString([]byte("sub")), Mode: "040000", SHA: hex.EncodeToString(dir)},
	}
	want := append(append([]byte("100644 a.go\x00"), blob...), append([]byte("40000 sub\x00"), dir...)...)
	if got := rawTree(ents); !bytes.Equal(got, want) {
		t.Errorf("rawTree = %q, want %q", got, want)
	}
}

func TestTheVfsdScenarioMountsReadsSnapshotsAndLandsAgainstTheService(t *testing.T) {
	t.Parallel()
	srv := testserver.Start(t)
	der, err := x509.MarshalPKCS8PrivateKey(srv.Operator.Key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	key := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	common := []string{"-url", srv.URL, "-key", key, "-kid", srv.Operator.Kid, "-out", dir}
	if err := runGen(t.Context(), append(common, "-objects", "1500", "-files", "100")); err != nil {
		t.Fatal(err)
	}
	if err := runLoad(t.Context(), append(common, "-scenario", "vfsd", "-c", "2", "-d", "2s", "-json", "-cold-rate", "0")); err != nil {
		t.Fatal(err)
	}
}
