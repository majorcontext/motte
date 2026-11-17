package server

import (
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

func bodyLength(n int64) int64 { return n }

// packCost is the body, its decoded copy and the working memory of pack verification.
func packCost(n int64) int64 { return n + n*3/4 + gitpack.VerifyWorkBytes }

// receive reads an upload body under the upload deadline and reserves cost(len(body)) bytes. The reader slot is held from before
// the first byte until the reservation is made. The caller releases the reservation.
func (s *Server) receive(w http.ResponseWriter, r *http.Request, limit int64, cost func(bodyLen int64) int64) ([]byte, func(), error) {
	select {
	case s.readers <- struct{}{}:
	case <-r.Context().Done():
		return nil, nil, r.Context().Err()
	}
	defer func() { <-s.readers }()
	if err := http.NewResponseController(w).SetReadDeadline(s.now().Add(uploadTimeout)); err != nil {
		return nil, nil, err
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
