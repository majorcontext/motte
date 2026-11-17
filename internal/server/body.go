package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

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

// readUpload is readBody under the upload deadline.
func (s *Server) readUpload(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, error) {
	if err := http.NewResponseController(w).SetReadDeadline(s.now().Add(uploadTimeout)); err != nil {
		return nil, err
	}
	return readBody(w, r, limit)
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
