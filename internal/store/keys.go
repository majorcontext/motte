package store

import (
	"crypto/rand"
	"encoding/hex"
	"path"
	"strings"
)

// writeID returns a random name for one write of a file. No two writes share it, so no write stores a key that an
// earlier write stored, and a collector that deletes an old key never removes a file that a later write stored.
func writeID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// uniqueKey adds the write id to the name of key, before its extension.
func uniqueKey(key, wid string) string {
	ext := path.Ext(key)
	return strings.TrimSuffix(key, ext) + "-" + wid + ext
}
