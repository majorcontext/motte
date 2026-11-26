package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/refname"
	"github.com/majorcontext/motte/internal/store"
)

const (
	smallBodyMax = 1 << 20
	blobsMax     = 4096
	treeDefault  = 4096
	opBodyMax    = store.MaxOpBytes*4/3 + 1024
	packBodyMax  = (store.MaxPackBytes+store.MaxIdxBytes)*4/3 + 4096
)

func (s *Server) registerStorePlane(mux *http.ServeMux) {
	reader := []grant.Scope{grant.ScopeNode, grant.ScopeRead}
	refs := []grant.Scope{grant.ScopeNode, grant.ScopeRead, grant.ScopeMerge}
	refWriter := []grant.Scope{grant.ScopeNode, grant.ScopeMerge}
	node := []grant.Scope{grant.ScopeNode}
	s.handle(mux, "GET /v1/vcs/commits/{sha}", s.getCommit, reader...)
	s.handle(mux, "GET /v1/vcs/trees/{sha}", s.getTrees, reader...)
	s.handle(mux, "GET /v1/vcs/objects/{sha}", s.getObject, reader...)
	s.handle(mux, "POST /v1/vcs/blobs/batch", s.getBlobs, reader...)
	s.handle(mux, "GET /v1/vcs/refs", s.listRefs, refs...)
	s.handle(mux, "GET /v1/vcs/refs/{name...}", s.getRef, refs...)
	s.handle(mux, "POST /v1/vcs/objects/has", s.hasObjects, grant.ScopeNode, grant.ScopeRead, grant.ScopeImport)
	s.handle(mux, "POST /v1/vcs/objects", s.putObjects, node...)
	s.handle(mux, "PUT /v1/vcs/ops/{kind}/{id}", s.putOp, node...)
	s.handle(mux, "GET /v1/vcs/ops/{kind}/{id}", s.getOp, node...)
	s.handle(mux, "GET /v1/vcs/op-heads", s.getOpHeads, node...)
	s.handle(mux, "PUT /v1/vcs/op-heads", s.swapOpHeads, node...)
	s.handle(mux, "PUT /v1/vcs/workspace", s.setWorkspace, node...)
	s.handle(mux, "PUT /v1/vcs/refs/{name...}", s.swapRef, refWriter...)
	s.handle(mux, "DELETE /v1/vcs/refs/{name...}", s.deleteRef, refWriter...)
	s.handle(mux, "PUT /v1/vcs/packs/{pack_id}", s.importPack, grant.ScopeImport)
}

func b64(data []byte) string { return base64.StdEncoding.EncodeToString(data) }

func unb64(s string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, store.ErrInvalid
	}
	return b, nil
}

func parseSHA(s string) (gitpack.SHA, error) {
	sha, err := gitpack.ParseSHA(s)
	if err != nil {
		return sha, store.ErrInvalid
	}
	return sha, nil
}

func parseSHAs(in []string) ([]gitpack.SHA, error) {
	if len(in) > blobsMax {
		return nil, store.ErrTooLarge
	}
	out := make([]gitpack.SHA, len(in))
	for i, h := range in {
		var err error
		if out[i], err = parseSHA(h); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// lowerHex reports whether s is lowercase hex of lo to hi digits.
func lowerHex(s string, lo, hi int) bool {
	if len(s) < lo || len(s) > hi {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func refName(r *http.Request) (string, error) {
	name := r.PathValue("name")
	if !refname.Valid(name) {
		return "", store.ErrInvalid
	}
	return name, nil
}

func (s *Server) getCommit(w http.ResponseWriter, r *http.Request, c call) error {
	release, err := s.reserve(r, reserveRead)
	if err != nil {
		return err
	}
	hold := s.holding(r, reserveRead, release)
	defer hold.drop()
	sha, err := parseSHA(r.PathValue("sha"))
	if err != nil {
		return err
	}
	info, err := s.objects.Commit(r.Context(), c.repo, sha, func(size int64) error { return hold.grow(readCost(size, 3)) })
	if err != nil {
		return err
	}
	parents := make([]string, len(info.Parents))
	for i, p := range info.Parents {
		parents[i] = p.String()
	}
	s.writeJSON(w, http.StatusOK, api.Commit{Tree: info.Tree.String(), Time: info.Time.Format(time.RFC3339), Parents: parents})
	return nil
}

func (s *Server) getTrees(w http.ResponseWriter, r *http.Request, c call) error {
	release, err := s.reserve(r, treeReserve)
	if err != nil {
		return err
	}
	hold := s.holding(r, treeReserve, release)
	defer hold.drop()
	grow := func(treeBytes int64) error { return hold.grow(treeCost(treeBytes)) }
	sha, err := parseSHA(r.PathValue("sha"))
	if err != nil {
		return err
	}
	limit := treeDefault
	if v := r.URL.Query().Get("max_entries"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > 100000 {
			return store.ErrInvalid
		}
	}
	trees, err := s.objects.TreeBulk(r.Context(), c.repo, sha, limit, grow)
	if err != nil {
		return err
	}
	out := api.Trees{Trees: make([]api.Tree, 0, len(trees))}
	for _, t := range trees {
		vt := api.Tree{SHA: t.SHA.String(), Entries: make([]api.TreeEntry, 0, len(t.Entries))}
		for _, e := range t.Entries {
			vt.Entries = append(vt.Entries, api.TreeEntry{Name: b64([]byte(e.Name)), Mode: strconv.FormatUint(uint64(e.Mode), 8), SHA: e.SHA.String(), Size: e.Size})
		}
		out.Trees = append(out.Trees, vt)
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getBlobs(w http.ResponseWriter, r *http.Request, c call) error {
	var req api.BlobsRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	release, err := s.reserve(r, reserveRead)
	if err != nil {
		return err
	}
	hold := s.holding(r, reserveRead, release)
	defer hold.drop()
	shas, err := parseSHAs(req.SHAs)
	if err != nil {
		return err
	}
	blobs, err := s.objects.Blobs(r.Context(), c.repo, shas, func(size int64) error { return hold.grow(readCost(size, 5)) })
	if err != nil {
		return err
	}
	out := api.Blobs{Blobs: make([]string, len(blobs))}
	for i, b := range blobs {
		out.Blobs[i] = b64(b)
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) hasObjects(w http.ResponseWriter, r *http.Request, c call) error {
	var req api.HasRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	release, err := s.reserve(r, reserveHas)
	if err != nil {
		return err
	}
	defer release()
	shas, err := parseSHAs(req.SHAs)
	if err != nil {
		return err
	}
	present, err := s.objects.Has(r.Context(), c.repo, shas)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, api.Has{Present: present})
	return nil
}

// aheadReader hands the decoder short reads and fails with store.ErrTooLarge when the decoder holds more than
// decodeWindow bytes of unparsed text, so one huge element cannot grow its buffer.
type aheadReader struct {
	r   io.Reader
	dec *json.Decoder
	n   int64
	// failed makes the refusal stick, because the decoder can read again after an error.
	failed bool
}

func (a *aheadReader) Read(p []byte) (int, error) {
	if a.failed {
		return 0, store.ErrTooLarge
	}
	n, err := a.r.Read(p[:min(len(p), 64<<10)])
	a.n += int64(n)
	if a.n-a.dec.InputOffset() > decodeWindow {
		a.failed = true
		return 0, store.ErrTooLarge
	}
	return n, err
}

func decodeFailure(err error) error {
	if errors.Is(err, store.ErrTooLarge) {
		return store.ErrTooLarge
	}
	return store.ErrInvalid
}

// decodeObjects reads {"objects":[...]} one object at a time, so a body full
// of empty objects is refused at the object limit before it is decoded.
func decodeObjects(body []byte) ([]gitpack.Object, error) {
	ahead := &aheadReader{r: bytes.NewReader(body)}
	dec := json.NewDecoder(ahead)
	ahead.dec = dec
	var objs []gitpack.Object
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil, decodeFailure(err)
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return nil, decodeFailure(err)
		}
		if key != "objects" {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return nil, decodeFailure(err)
			}
			continue
		}
		if t, err := dec.Token(); err != nil || t != json.Delim('[') {
			return nil, decodeFailure(err)
		}
		for dec.More() {
			if len(objs) >= store.MaxBatch {
				return nil, store.ErrInvalid
			}
			var o api.Object
			if err := dec.Decode(&o); err != nil {
				return nil, decodeFailure(err)
			}
			data, err := unb64(o.Data)
			if err != nil {
				return nil, err
			}
			if len(data) > objectLimit {
				return nil, store.ErrTooLarge
			}
			objs = append(objs, gitpack.Object{Type: gitpack.ParseType(o.Type), Data: data})
		}
		if _, err := dec.Token(); err != nil {
			return nil, decodeFailure(err)
		}
	}
	if t, err := dec.Token(); err != nil || t != json.Delim('}') {
		return nil, decodeFailure(err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, decodeFailure(err)
	}
	return objs, nil
}

func (s *Server) putObjects(w http.ResponseWriter, r *http.Request, c call) error {
	body, release, err := s.receive(w, r, packBodyMax, objectsCost)
	if err != nil {
		return err
	}
	defer release()
	objs, err := decodeObjects(body)
	if err != nil {
		return err
	}
	names, err := s.objects.PutObjects(r.Context(), c.repo, objs)
	if err != nil {
		return err
	}
	out := api.PutObjectsResponse{SHAs: make([]string, len(names))}
	for i, n := range names {
		out.SHAs[i] = n.String()
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getObject(w http.ResponseWriter, r *http.Request, c call) error {
	release, err := s.reserve(r, reserveRead)
	if err != nil {
		return err
	}
	hold := s.holding(r, reserveRead, release)
	defer hold.drop()
	sha, err := parseSHA(r.PathValue("sha"))
	if err != nil {
		return err
	}
	rd, err := s.objects.Reader(r.Context(), c.repo)
	if err != nil {
		return err
	}
	t, data, err := rd.GetSized(r.Context(), sha, func(size int64) error { return hold.grow(readCost(size, 5)) })
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, api.Object{Type: t.String(), Data: b64(data)})
	return nil
}

func (s *Server) importPack(w http.ResponseWriter, r *http.Request, c call) error {
	body, release, err := s.receive(w, r, packBodyMax, packCost)
	if err != nil {
		return err
	}
	defer release()
	var req api.ImportPackRequest
	if err := decodeJSON(body, &req); err != nil {
		return err
	}
	idx, err := unb64(req.Index)
	if err != nil {
		return err
	}
	pack, err := unb64(req.Pack)
	if err != nil {
		return err
	}
	if err := s.objects.ImportPack(r.Context(), c.repo, r.PathValue("pack_id"), idx, pack); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) getOp(w http.ResponseWriter, r *http.Request, c call) error {
	release, err := s.reserve(r, reserveOp)
	if err != nil {
		return err
	}
	defer release()
	data, err := s.objects.GetOp(r.Context(), c.claims.WS, store.OpKind(r.PathValue("kind")), r.PathValue("id"))
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, api.OpObject{Data: b64(data)})
	return nil
}

func (s *Server) putOp(w http.ResponseWriter, r *http.Request, c call) error {
	body, release, err := s.receive(w, r, opBodyMax, opCost)
	if err != nil {
		return err
	}
	defer release()
	var req api.OpObject
	if err := decodeJSON(body, &req); err != nil {
		return err
	}
	data, err := unb64(req.Data)
	if err != nil {
		return err
	}
	if err := s.objects.PutOp(r.Context(), c.claims.WS, store.OpKind(r.PathValue("kind")), r.PathValue("id"), data); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) getOpHeads(w http.ResponseWriter, r *http.Request, c call) error {
	h, err := s.index.OpHeads(r.Context(), c.claims.WS)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, opHeadsView(h))
	return nil
}

func (s *Server) swapOpHeads(w http.ResponseWriter, r *http.Request, c call) error {
	var req api.SwapOpHeadsRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	if len(req.Heads) == 0 || req.ExpectGeneration < 0 {
		return store.ErrInvalid
	}
	h, err := s.index.SwapOpHeads(r.Context(), c.claims.WS, c.claims.Epoch, c.claims.Repo, req.ExpectGeneration, req.Heads)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, opHeadsView(h))
	return nil
}

func (s *Server) setWorkspace(w http.ResponseWriter, r *http.Request, c call) error {
	var req api.SetWorkspaceRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	if !lowerHex(req.Op, 16, 128) || (len(req.Commit) != 40 && len(req.Commit) != 64) || !lowerHex(req.Commit, 40, 64) || req.Generation <= 0 {
		return store.ErrInvalid
	}
	if err := s.index.Report(r.Context(), c.claims.WS, c.claims.Epoch, c.claims.Repo, req.Generation, "", req.Op, req.Commit); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Server) listRefs(w http.ResponseWriter, r *http.Request, c call) error {
	refs, err := s.index.Refs(r.Context(), c.claims.Repo)
	if err != nil {
		return err
	}
	out := api.Refs{Refs: make([]api.Ref, len(refs))}
	for i, ref := range refs {
		out.Refs[i] = refView(ref)
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) getRef(w http.ResponseWriter, r *http.Request, c call) error {
	name, err := refName(r)
	if err != nil {
		return err
	}
	ref, err := s.index.Ref(r.Context(), c.claims.Repo, name)
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, refView(ref))
	return nil
}

func (s *Server) swapRef(w http.ResponseWriter, r *http.Request, c call) error {
	name, err := refName(r)
	if err != nil {
		return err
	}
	var req api.SwapRefRequest
	if err := decodeBody(w, r, smallBodyMax, &req); err != nil {
		return err
	}
	if req.Expect < 0 || !lowerHex(req.Commit, 40, 40) {
		return store.ErrInvalid
	}
	ref, err := s.writeRefAs(r.Context(), c, refWrite{Name: name, Expect: req.Expect, SHA: req.Commit})
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, refView(ref))
	return nil
}

func (s *Server) deleteRef(w http.ResponseWriter, r *http.Request, c call) error {
	name, err := refName(r)
	if err != nil {
		return err
	}
	expect, err := strconv.ParseInt(r.URL.Query().Get("expect"), 10, 64)
	if err != nil || expect < 1 {
		return store.ErrInvalid
	}
	if _, err := s.writeRefAs(r.Context(), c, refWrite{Name: name, Expect: expect, Remove: true}); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}
