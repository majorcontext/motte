package server

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/store"
)

const uploadTimeout = 2 * time.Minute

// readBody reads the whole body, refusing more than limit bytes with store.ErrTooLarge. A handler reads its body
// before it reserves memory, so a slow sender holds no reservation.
func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		return nil, store.ErrTooLarge
	case err != nil:
		return nil, errBody
	}
	return raw, nil
}

// uploadReaders bounds the upload bodies that are being read or wait for a reservation, so the memory of bodies is
// at most uploadReaders times the largest body limit.
const uploadReaders = 4

// The reservation of a request is the peak of its live buffers, derived from the code path. B is the body length.
//
//	read of one commit      3 times its size, at least 4 MiB: the delta, its base and its result
//	read of trees           the tree bytes read so far, 8 bytes of views and JSON for each, and 16 MiB of slack; grown as trees are met
//	read of one op          8 MiB data, its base64 text and the encoder buffer
//	read of one object or blob batch
//	                        16 MiB data, its base64 text, and an encoder buffer that has doubled once
//	has                     the SHA lists only
//	put objects             B, then the 80 MiB decoder slack, the decoded objects (3B/4), the pack body and the pack (each 3B/4 plus 2 percent), and the index
//	put op                  the body, the Data string and the decoded op: 3B
//	import pack             the body, the strings of both fields, the decoded pack and index (3B/4), and the verification work
const (
	reserveHas  = 1 << 20
	reserveOp   = 5 * store.MaxOpBytes
	reserveRead = 4 << 20
	// decodeSlack is the decoder buffer (twice the window and one read), the text of one object and the pack index.
	decodeSlack = 80 << 20
)

// The size limits that the limit tests lower through LowerLimits. Each starts at the value of the spec.
var (
	// objectLimit is the largest object that a put accepts.
	objectLimit = gitpack.MaxObjectBytes
	// decodeWindow bounds the JSON text that the decoder of decodeObjects holds for one object: 16 MiB in base64 and
	// some slack.
	decodeWindow int64 = 24 << 20
	// treeReserve is the first reservation of a tree read, and treeSlack the fixed part of every later one.
	treeReserve int64 = 48 << 20
	treeSlack   int64 = 16 << 20
)

// treeCost is the reservation of a tree read that holds treeBytes of tree data.
func treeCost(treeBytes int64) int64 { return 8*treeBytes + treeSlack }

// readCost is the reservation of a read of size bytes of objects, grown as the sizes are met. A commit read holds the
// delta, its base and its result: 3 times. An object or blob read holds the data, its base64 text and an encoder
// buffer that doubled once: 5 times.
func readCost(size, times int64) int64 { return max(reserveRead, times*size) }

func objectsCost(n int64) int64 { return n + 5*n/2 + decodeSlack }

func opCost(n int64) int64 { return 3 * n }

func packCost(n int64) int64 { return 3*n + gitpack.VerifyWorkBytes }

// receive reads an upload body under the upload deadline and reserves cost(len(body)) bytes. The reader slot is held from before
// the first byte until the reservation is made. The caller releases the reservation.
func (s *Server) receive(w http.ResponseWriter, r *http.Request, limit int64, cost func(bodyLen int64) int64) ([]byte, func(), error) {
	return s.receiveEncoded(w, r, limit, cost, false)
}

// receiveEncoded is receive that inflates a gzip body when inflate is set. The gzip header is read under the reader
// slot and the upload deadline, like the rest of the body, and the limit applies to the inflated bytes.
func (s *Server) receiveEncoded(w http.ResponseWriter, r *http.Request, limit int64, cost func(bodyLen int64) int64, inflate bool) ([]byte, func(), error) {
	select {
	case s.readers <- struct{}{}:
	case <-r.Context().Done():
		return nil, nil, r.Context().Err()
	}
	defer func() { <-s.readers }()
	if err := http.NewResponseController(w).SetReadDeadline(s.now().Add(uploadTimeout)); err != nil {
		return nil, nil, err
	}
	if inflate && r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, nil, errBody
		}
		defer func() { _ = zr.Close() }()
		r.Body = zr
	}
	body, err := readBody(w, r, limit)
	if err != nil {
		return nil, nil, err
	}
	release, err := s.reserve(r, cost(int64(len(body))))
	return body, release, err
}

func decodeJSON(raw []byte, out any) error {
	if len(raw) == 0 || json.Unmarshal(raw, out) != nil {
		return errBody
	}
	return nil
}

func decodeBody(w http.ResponseWriter, r *http.Request, limit int64, out any) error {
	raw, err := readBody(w, r, limit)
	if err != nil {
		return err
	}
	return decodeJSON(raw, out)
}
