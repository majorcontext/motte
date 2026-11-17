package ctl

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/vfs/store"
)

// TestCtlRefsSwapAgainstTheRefRoutes drives the refs and swapref ops through handleCtl and HTTPStore against a
// fake of the ref routes: a stale swap must answer the code with the stored ref, and a bad shape must not call.
func TestCtlRefsSwapAgainstTheRefRoutes(t *testing.T) {
	stored, calls := map[string]api.Ref{}, 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		name := strings.TrimPrefix(r.URL.Path, "/v1/vcs/refs")
		name = strings.TrimPrefix(name, "/")
		cur, have := stored[name]
		fail := func(status int, code string, ref *api.Ref) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": code, "details": map[string]any{"ref": ref}}})
		}
		if name == "heads/moved" {
			http.Redirect(w, r, "/v1/vcs/refs/heads/elsewhere", http.StatusTemporaryRedirect)
			return
		}
		var rq api.SwapRefRequest
		_ = json.NewDecoder(r.Body).Decode(&rq)
		switch {
		case r.Method == http.MethodGet && name == "":
			refs := []api.Ref{}
			for _, ref := range stored {
				refs = append(refs, ref)
			}
			_ = json.NewEncoder(w).Encode(api.Refs{Refs: refs})
		case r.Method == http.MethodGet && have:
			_ = json.NewEncoder(w).Encode(cur)
		case r.Method == http.MethodGet:
			fail(http.StatusNotFound, "vcs_not_found", nil)
		case r.Method == http.MethodPut && cur.Generation == rq.Expect:
			stored[name] = api.Ref{Name: name, Commit: rq.Commit, Generation: int64(calls)}
			_ = json.NewEncoder(w).Encode(stored[name])
		case r.Method == http.MethodDelete && r.URL.Query().Get("expect") == "1":
			delete(stored, name)
			w.WriteHeader(http.StatusNoContent)
		default:
			var ref *api.Ref
			if have {
				ref = &cur
			}
			fail(http.StatusConflict, "vcs_generation_conflict", ref)
		}
	}))
	defer srv.Close()
	v := &Server{Store: &store.HTTPStore{BaseURL: srv.URL, Grant: func() (string, error) { return "g", nil }}}
	c1, c2 := strings.Repeat("a", 40), strings.Repeat("b", 40)
	wide := "heads/" + strings.Repeat("<>&", 83)
	steps := []struct {
		name  string
		rq    ctlReq
		err   string
		ref   *api.Ref
		refs  []api.Ref
		calls int
	}{
		{name: "create", rq: ctlReq{Op: "swapref", Name: "heads/main", Commit: c1}, ref: &api.Ref{Name: "heads/main", Commit: c1, Generation: 1}, calls: 1},
		{name: "read", rq: ctlReq{Op: "refs", Name: "heads/main"}, ref: &api.Ref{Name: "heads/main", Commit: c1, Generation: 1}, calls: 1},
		{name: "list", rq: ctlReq{Op: "refs"}, refs: []api.Ref{{Name: "heads/main", Commit: c1, Generation: 1}}, calls: 1},
		{name: "stale swap", rq: ctlReq{Op: "swapref", Name: "heads/main", Expect: 7, Commit: c2}, err: "vcs_generation_conflict", ref: &api.Ref{Name: "heads/main", Commit: c1, Generation: 1}, calls: 1},
		{name: "absent", rq: ctlReq{Op: "refs", Name: "heads/gone"}, err: "vcs_not_found", calls: 1},
		{name: "bad commit", rq: ctlReq{Op: "swapref", Name: "heads/main", Expect: 1, Commit: "ABC"}, err: "40 lowercase hex"},
		{name: "empty name", rq: ctlReq{Op: "swapref", Commit: c1}, err: "needs a name"},
		{name: "dotted segment", rq: ctlReq{Op: "swapref", Name: "heads/../tags/v1", Commit: c1}, err: "invalid ref name"},
		{name: "empty segment", rq: ctlReq{Op: "swapref", Name: "heads//main", Expect: 1, Delete: true}, err: "invalid ref name"},
		{name: "dot read", rq: ctlReq{Op: "refs", Name: "heads/./main"}, err: "invalid ref name"},
		{name: "redirect", rq: ctlReq{Op: "swapref", Name: "heads/moved", Commit: c1}, err: "307", calls: 1},
		{name: "wide create", rq: ctlReq{Op: "swapref", Name: wide, Commit: c1}, ref: &api.Ref{Name: wide, Commit: c1, Generation: 7}, calls: 1},
		{name: "wide conflict", rq: ctlReq{Op: "swapref", Name: wide, Expect: 3, Commit: c2}, err: "vcs_generation_conflict", ref: &api.Ref{Name: wide, Commit: c1, Generation: 7}, calls: 1},
		{name: "delete without expect", rq: ctlReq{Op: "swapref", Name: "heads/main", Delete: true}, err: "stored generation"},
		{name: "delete", rq: ctlReq{Op: "swapref", Name: "heads/main", Expect: 1, Delete: true}, calls: 1},
	}
	for _, s := range steps {
		before := calls
		rs := v.handle(t.Context(), s.rq)
		if !strings.Contains(rs.Err, s.err) || (s.err == "" && rs.Err != "") || !reflect.DeepEqual(rs.Ref, s.ref) || !reflect.DeepEqual(rs.Refs, s.refs) || calls-before != s.calls {
			t.Errorf("%s: answer %+v after %d calls, want err %q ref %+v refs %+v and %d calls", s.name, rs, calls-before, s.err, s.ref, s.refs, s.calls)
		}
	}
}
