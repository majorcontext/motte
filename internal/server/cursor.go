package server

import (
	"encoding/base64"
	"strconv"
	"strings"

	"github.com/majorcontext/motte/meta"
)

// encodeCursor is the opaque form of a position in every event partition: the base64url text of one decimal Seq for
// each partition, joined by commas.
func encodeCursor(seqs []int64) string {
	parts := make([]string, len(seqs))
	for i, n := range seqs {
		parts[i] = strconv.FormatInt(n, 10)
	}
	return base64.RawURLEncoding.EncodeToString([]byte(strings.Join(parts, ",")))
}

// decodeCursor reads a cursor. The empty text is the start of every partition.
func decodeCursor(text string) ([]int64, error) {
	seqs := make([]int64, meta.Partitions)
	if text == "" {
		return seqs, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(text)
	if err != nil {
		return nil, errBody
	}
	parts := strings.Split(string(raw), ",")
	if len(parts) != meta.Partitions {
		return nil, errBody
	}
	for i, p := range parts {
		if seqs[i], err = strconv.ParseInt(p, 10, 64); err != nil || seqs[i] < 0 {
			return nil, errBody
		}
	}
	return seqs, nil
}
