package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/majorcontext/motte/grant"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/gitproto"
	"github.com/majorcontext/motte/internal/repos"
	"github.com/majorcontext/motte/internal/store"
	"github.com/majorcontext/motte/meta"
)

const (
	gitChallenge = `Basic realm="motte"`
	// fetchBodyMax bounds the wants and haves of one fetch request.
	fetchBodyMax = 8 << 20
	// pushBodyMax bounds the commands and the pack of one push.
	pushBodyMax = store.MaxPackBytes + 1<<20
	// A fetch holds the ranges that its read-ahead has in flight (fetchAhead), 3 times its largest object so far
	// (fetchObject at first), and fetchPerObject for each object of its sets.
	fetchAhead     = store.ReadAheadBytes
	fetchObject    = 1 << 20
	fetchPerObject = 128
)

// fetchHold is the reservation of a fetch.
type fetchHold struct {
	*reservation
	objects int
	largest int64
}

func (f *fetchHold) cost() int64 {
	return fetchAhead + 3*f.largest + fetchPerObject*int64(f.objects)
}

func (f *fetchHold) track(objects int) error {
	f.objects = objects
	return f.grow(f.cost())
}

func (f *fetchHold) object(size int64) error {
	if size <= f.largest {
		return nil
	}
	f.largest = size
	return f.grow(f.cost())
}

// pushCost is the reservation of a push whose body has n bytes and whose unpack holds inflated bytes: the body, the
// entries, the objects and the rebuilt pack (3 times the inflated bytes), the delta, its base and its result while one
// object resolves (4 times the largest object, which is at most the inflated bytes), and the fixed buffers.
func pushCost(n, inflated int64) int64 {
	return n + 3*inflated + 4*min(inflated, gitpack.MaxObjectBytes) + pushBase
}

// pushStep is the granule of the growing reservation of a push, and pushBase its fixed part.
const (
	pushStep = 1 << 20
	pushBase = 2 << 20
)

var errProtocolV2 = gitproto.RequestError("protocol v2 is required: use git 2.26 or later")

func (s *Server) registerGit(mux *http.ServeMux) {
	scopes := []grant.Scope{grant.ScopeRead, grant.ScopeWrite, grant.ScopeMerge}
	s.register(mux, "GET /git/{path...}", s.gitInfoRefs, scopes, s.gitAuthenticate, gitChallenge)
	s.register(mux, "POST /git/{path...}", s.gitService, scopes, s.gitAuthenticate, gitChallenge)
}

// gitAuthenticate tries the grant of the request, then a person: the identity of the server, then a per-user token.
func (s *Server) gitAuthenticate(r *http.Request) (grant.Claims, error) {
	repo, _, err := gitRoute(r.PathValue("path"))
	text := ""
	if err == nil {
		text = repo.String()
	}
	if c, err := s.bearerGit(r); err == nil {
		return c, nil
	}
	if text != "" {
		if c, err := s.gitHuman.Authenticate(r, text); err == nil {
			return c, nil
		}
	}
	return grant.Claims{}, errUnauthorized
}

// gitToken reads the grant from a Bearer header, or from the password of a Basic header.
func gitToken(r *http.Request) string {
	if tok := bearerToken(r); tok != "" {
		return tok
	}
	if _, pass, ok := r.BasicAuth(); ok {
		return pass
	}
	return ""
}

// gitRoute splits a /git path into the repository and the part after <path>.git/. The repository path of the URL is
// <prefix>/<path> and the grant names it as <prefix>:<path>.
func gitRoute(path string) (repo store.Repo, rest string, err error) {
	i := strings.LastIndex(path, ".git/")
	if i < 0 {
		return store.Repo{}, "", store.ErrNotFound
	}
	prefix, p, ok := strings.Cut(path[:i], "/")
	if !ok {
		return store.Repo{}, "", store.ErrNotFound
	}
	repo, err = store.ParseRepo(prefix + ":" + p)
	if err != nil {
		return store.Repo{}, "", store.ErrNotFound
	}
	return repo, path[i+len(".git/"):], nil
}

// gitCall resolves the repository of the URL, checks that it is the repository of the grant, and that the
// repository exists.
func (s *Server) gitCall(r *http.Request, c call) (string, error) {
	repo, rest, err := gitRoute(r.PathValue("path"))
	if err != nil {
		return "", err
	}
	if repo != c.repo {
		return "", errForbidden
	}
	if repo.Prefix == repos.Prefix {
		if _, err := s.index.Repo(r.Context(), repo.Path); err != nil {
			return "", err
		}
	} else if refs, err := s.index.Refs(r.Context(), repo.String()); err != nil || len(refs) == 0 {
		if err == nil {
			err = store.ErrNotFound
		}
		return "", err
	}
	return rest, nil
}

func requireWrite(c call) error {
	if c.claims.Scope != grant.ScopeWrite && c.claims.Scope != grant.ScopeMerge {
		return errNotWritable
	}
	return nil
}

// protocolVersion reads the version of the Git-Protocol header, 0 when it has none.
func protocolVersion(r *http.Request) string {
	for _, kv := range strings.Split(r.Header.Get("Git-Protocol"), ":") {
		if v, ok := strings.CutPrefix(kv, "version="); ok {
			return v
		}
	}
	return "0"
}

func gitHeaders(w http.ResponseWriter, contentType string) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-cache")
}

func (s *Server) gitInfoRefs(w http.ResponseWriter, r *http.Request, c call) error {
	rest, err := s.gitCall(r, c)
	if err != nil {
		return err
	}
	if rest != "info/refs" {
		return store.ErrNotFound
	}
	switch r.URL.Query().Get("service") {
	case "git-upload-pack":
		if protocolVersion(r) != "2" {
			return errProtocolV2
		}
		gitHeaders(w, "application/x-git-upload-pack-advertisement")
		return gitproto.AdvertiseUpload(w)
	case "git-receive-pack":
		if err := requireWrite(c); err != nil {
			return err
		}
		refs, _, err := s.gitRefs(r.Context(), c.repo)
		if err != nil {
			return err
		}
		version := 0
		if protocolVersion(r) == "1" {
			version = 1
		}
		gitHeaders(w, "application/x-git-receive-pack-advertisement")
		return gitproto.AdvertiseReceive(w, refs, version)
	}
	return gitproto.RequestError("only the smart protocol is served: the query needs service=git-upload-pack or git-receive-pack")
}

// gitRefs lists the refs of a repository under their git names, and the git name that HEAD names.
func (s *Server) gitRefs(ctx context.Context, repo store.Repo) ([]gitproto.Ref, string, error) {
	stored, err := s.index.Refs(ctx, repo.String())
	if err != nil {
		return nil, "", err
	}
	var refs []gitproto.Ref
	head := ""
	for _, ref := range stored {
		name, ok := gitproto.GitRefName(ref.Name)
		sha, err := gitpack.ParseSHA(ref.Commit)
		if !ok || err != nil {
			continue
		}
		if name == "refs/heads/"+repos.DefaultBranch {
			head = name
		}
		refs = append(refs, gitproto.Ref{Name: name, SHA: sha})
	}
	slices.SortFunc(refs, func(a, b gitproto.Ref) int { return strings.Compare(a.Name, b.Name) })
	return refs, head, nil
}

// gitBody reads a request body of at most limit bytes, inflated when the client compressed it, and reserves cost.
func (s *Server) gitBody(w http.ResponseWriter, r *http.Request, limit int64, cost func(int64) int64) ([]byte, func(), error) {
	return s.receiveEncoded(w, r, limit, cost, true)
}

func (s *Server) gitService(w http.ResponseWriter, r *http.Request, c call) error {
	rest, err := s.gitCall(r, c)
	if err != nil {
		return err
	}
	switch rest {
	case "git-upload-pack":
		return s.uploadPack(w, r, c)
	case "git-receive-pack":
		if err := requireWrite(c); err != nil {
			return err
		}
		return s.receivePack(w, r, c)
	}
	return store.ErrNotFound
}

// abortAfterHeaders ends a response that has started and then failed: the client sees a broken stream, never a
// clean end of a truncated pack.
func (s *Server) abortAfterHeaders(w http.ResponseWriter, r *http.Request, err error) error {
	if sw, ok := w.(*statusWriter); ok && sw.wrote && err != nil {
		s.logger.ErrorContext(r.Context(), "git response failed", "route", r.Pattern, "error", logSafe(err))
		panic(http.ErrAbortHandler)
	}
	return err
}

func (s *Server) uploadPack(w http.ResponseWriter, r *http.Request, c call) error {
	if protocolVersion(r) != "2" {
		return errProtocolV2
	}
	fh := &fetchHold{largest: fetchObject}
	body, release, err := s.gitBody(w, r, fetchBodyMax, func(int64) int64 { return fh.cost() })
	if err != nil {
		return err
	}
	fh.reservation = s.holding(r, fh.cost(), release)
	defer fh.drop()
	refs, head, err := s.gitRefs(r.Context(), c.repo)
	if err != nil {
		return err
	}
	rd, err := s.objects.Reader(r.Context(), c.repo)
	if err != nil {
		return err
	}
	gitHeaders(w, "application/x-git-upload-pack-result")
	up := &gitproto.Upload{Objects: reader{rd, fh.object}, Refs: refs, Head: head, Track: fh.track}
	return s.abortAfterHeaders(w, r, up.Serve(r.Context(), w, bytes.NewReader(body)))
}

// reader adapts the object reader of the store to gitproto.
type reader struct {
	r      *store.Reader
	before func(size int64) error
}

func (o reader) Get(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	t, data, err := o.r.GetSized(ctx, sha, o.before)
	if errors.Is(err, store.ErrNotFound) {
		err = gitproto.ErrNotFound
	}
	return t, data, err
}

func (o reader) Prefetch(ctx context.Context, shas []gitpack.SHA) { o.r.Prefetch(ctx, shas) }

func (o reader) Has(ctx context.Context, shas []gitpack.SHA) ([]bool, error) {
	return o.r.Has(ctx, shas)
}

func (s *Server) receivePack(w http.ResponseWriter, r *http.Request, c call) error {
	var bodyLen int64
	body, release, err := s.gitBody(w, r, pushBodyMax, func(n int64) int64 { bodyLen = n; return pushCost(n, 0) })
	if err != nil {
		return err
	}
	hold := s.holding(r, pushCost(bodyLen, 0), release)
	defer hold.drop()
	push, err := gitproto.ParsePush(body)
	if err != nil {
		return err
	}
	unpackErr, err := s.storePush(r.Context(), c, push, func(inflated int64) error {
		return hold.grow(pushCost(bodyLen, (inflated+pushStep-1)/pushStep*pushStep))
	})
	if err != nil {
		return err
	}
	results := make([]gitproto.Result, len(push.Commands))
	for i, cmd := range push.Commands {
		results[i] = gitproto.Result{Ref: cmd.Ref, Reason: unpackErr}
		if unpackErr == "" {
			if results[i].Reason, err = s.applyCommand(r.Context(), c, cmd); err != nil {
				return err
			}
		} else {
			results[i].Reason = "unpacker error"
		}
	}
	gitHeaders(w, "application/x-git-receive-pack-result")
	if !push.ReportStatus {
		return nil
	}
	return s.abortAfterHeaders(w, r, gitproto.WriteReport(w, unpackErr, results))
}

// storePush stores the pack of a push. A refused pack is the text of the report, and any other failure is an error.
func (s *Server) storePush(ctx context.Context, c call, push gitproto.Push, held func(int64) error) (string, error) {
	if !slices.ContainsFunc(push.Commands, func(cmd gitproto.Command) bool { return cmd.New != gitproto.Zero }) {
		return "", nil
	}
	if len(push.Pack) == 0 {
		return "missing pack", nil
	}
	err := s.objects.ReceivePack(ctx, c.repo, push.Pack, held)
	switch {
	case errors.Is(err, store.ErrInvalid):
		return "invalid pack", nil
	case errors.Is(err, store.ErrTooLarge):
		return "pack too large", nil
	}
	return "", err
}

// applyCommand checks and applies one ref command. It returns the reason that the command fails, or empty.
func (s *Server) applyCommand(ctx context.Context, c call, cmd gitproto.Command) (string, error) {
	name, ok := gitproto.StoredRefName(cmd.Ref)
	if !ok || cmd.Old == gitproto.Zero && cmd.New == gitproto.Zero {
		return "refusing to update this ref", nil
	}
	if cmd.New != gitproto.Zero {
		rd, err := s.objects.Reader(ctx, c.repo)
		if err != nil {
			return "", err
		}
		t, _, err := rd.Get(ctx, cmd.New)
		switch {
		case errors.Is(err, store.ErrNotFound):
			return "missing necessary objects", nil
		case err != nil:
			return "", err
		case t != gitpack.Commit && (t != gitpack.Tag || strings.HasPrefix(cmd.Ref, "refs/heads/")):
			return "the new value is not a commit", nil
		}
	}
	var conflict *meta.RefConflict
	err := s.updateRef(ctx, c, name, cmd.Old, cmd.New)
	switch {
	case errors.As(err, &conflict):
		return "stale info", nil
	case errors.Is(err, errProtected):
		return "protected", nil
	}
	return "", err
}

// updateRef is the one write of a ref that a direct push makes. It is a compare and swap on the commit that the
// client saw: a zero old creates the ref and a zero new deletes it. It goes through writeRefAs, so a protected ref
// refuses a write grant.
func (s *Server) updateRef(ctx context.Context, c call, name string, old, new gitpack.SHA) error {
	_, err := s.writeRefAs(ctx, c, refWrite{Name: name, ByCommit: true, Old: shaText(old), New: shaText(new)})
	return err
}

func shaText(sha gitpack.SHA) string {
	if sha == gitproto.Zero {
		return ""
	}
	return sha.String()
}
