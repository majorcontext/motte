package server

import (
	"net/http"
	"strings"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/store"
)

// fileMax is the most bytes of one file that the file read returns.
const fileMax = 1 << 20

func (s *Server) getFile(w http.ResponseWriter, r *http.Request, c call) error {
	if r.PathValue("repo") != c.claims.Repo {
		return errForbidden
	}
	sha, err := parseSHA(r.PathValue("commit"))
	if err != nil {
		return err
	}
	path := r.PathValue("path")
	for seg := range strings.SplitSeq(path, "/") {
		if seg == "" || seg == "." || seg == ".." || strings.ContainsRune(seg, 0) {
			return store.ErrInvalid
		}
	}
	release, err := s.reserve(r, reserveRead)
	if err != nil {
		return err
	}
	hold := s.holding(r, reserveRead, release)
	defer hold.drop()
	data, err := s.objects.File(r.Context(), c.repo, sha, path, fileMax, func(size int64) error { return hold.grow(readCost(size, 3)) })
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, api.File{SHA: gitpack.Hash(gitpack.Blob, data).String(), Size: int64(len(data)), Data: b64(data)})
	return nil
}
