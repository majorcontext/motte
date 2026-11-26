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

// packIDOfKey returns the pack id of a key of a pack file or an index file: the name without its write id and extension.
func packIDOfKey(key string) string {
	name := strings.TrimSuffix(path.Base(key), path.Ext(key))
	id, _, _ := strings.Cut(name, "-")
	return id
}
