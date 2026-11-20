package main

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/majorcontext/motte/grant"
)

// runMint is `motte operator-grant --key FILE --kid KID [--ttl D]`. It prints an operator grant that the key signs,
// so an administrator needs no other system to start.
func runMint(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("operator-grant", flag.ContinueOnError)
	fs.SetOutput(stderr)
	keyFile := fs.String("key", "", "PEM file of the Ed25519 private key")
	kid := fs.String("kid", "", "key ID that MOTTE_TRUSTED_KEYS lists")
	ttl := fs.Duration("ttl", time.Hour, "lifetime of the grant")
	if fs.Parse(args) != nil || fs.NArg() != 0 || *keyFile == "" || *kid == "" || *ttl <= 0 {
		fmt.Fprintln(stderr, "usage: motte operator-grant --key FILE --kid KID [--ttl D]")
		return 2
	}
	tok, err := mintOperator(*keyFile, *kid, *ttl)
	if err != nil {
		fmt.Fprintln(stderr, "motte operator-grant:", err)
		return 1
	}
	fmt.Fprintln(stdout, tok)
	return 0
}

func mintOperator(keyFile, kid string, ttl time.Duration) (string, error) {
	raw, err := os.ReadFile(keyFile)
	if err != nil {
		return "", err
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		return "", errors.New("the key file holds no PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	priv, ok := key.(ed25519.PrivateKey)
	if err != nil || !ok {
		return "", errors.New("the key is not an Ed25519 PKCS#8 key")
	}
	return grant.Signer{Kid: kid, Key: priv}.Mint(grant.Claims{Scope: grant.ScopeOperator, Exp: time.Now().Add(ttl).Unix()})
}
