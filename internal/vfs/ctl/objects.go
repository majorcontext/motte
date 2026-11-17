package ctl

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/store"
)

const maxObjects = 4096

func tooMany() failure { return invalid("ctl: more than %d objects in one request", maxObjects) }

// parseSHAs returns the names as SHAs. At most maxObjects names are allowed.
func parseSHAs(names []string) ([]gitpack.SHA, *failure) {
	if len(names) > maxObjects {
		f := tooMany()
		return nil, &f
	}
	shas := make([]gitpack.SHA, len(names))
	for i, n := range names {
		var err error
		if shas[i], err = gitpack.ParseSHA(n); err != nil {
			f := invalid("ctl: %v", err)
			return nil, &f
		}
	}
	return shas, nil
}

func serveHas(ctx context.Context, s store.HasStore, names []string) any {
	shas, bad := parseSHAs(names)
	if bad != nil {
		return *bad
	}
	present, err := s.Has(ctx, shas)
	if err != nil {
		return storeFail(err)
	}
	return presentAnswer{nonNil(present)}
}

// serveWorkspace answers workspace: rq.SHA is the head operation, rq.Commit the commit of the mount and rq.Generation
// the op-heads generation of the swap that set the operation.
func serveWorkspace(ctx context.Context, s store.WorkspaceStore, rq request) any {
	ctx, cancel := context.WithTimeout(ctx, store.OpHeadsBudget)
	defer cancel()
	if err := s.PutWorkspace(ctx, rq.SHA, rq.Commit, rq.Generation); err != nil {
		return storeFail(err)
	}
	return empty{}
}

// serveOp stores or reads one object of the op log: Type is the kind ("operations" or "views") and SHA the ID.
func serveOp(ctx context.Context, s store.OpObjectStore, rq request) any {
	if rq.Op == "getop" {
		data, err := s.GetOp(ctx, rq.Type, rq.SHA)
		if err != nil {
			return storeFail(err)
		}
		return dataAnswer{base64.StdEncoding.EncodeToString(data)}
	}
	data, err := base64.StdEncoding.DecodeString(rq.Data)
	if err != nil {
		return invalid("ctl: data is not base64: %v", err)
	}
	if err := s.PutOp(ctx, rq.Type, rq.SHA, data); err != nil {
		return storeFail(err)
	}
	return empty{}
}

// objects serves get, put and putbatch. The names of put objects are computed here, so a client cannot name an
// object it did not write.
func (s *Server) objects(ctx context.Context, rq request) any {
	switch rq.Op {
	case "get":
		shas, bad := parseSHAs([]string{rq.SHA})
		if bad != nil {
			return *bad
		}
		t, data, err := s.Store.Object(ctx, shas[0])
		if err != nil {
			return storeFail(err)
		}
		return objectAnswer{t.String(), base64.StdEncoding.EncodeToString(data)}
	case "put":
		shas, err := s.putObjects(ctx, []api.Object{{Type: rq.Type, Data: rq.Data}})
		if err != nil {
			return err
		}
		return shaAnswer{shas[0]}
	}
	shas, err := s.putObjects(ctx, rq.Objects)
	if err != nil {
		return err
	}
	return shasAnswer{shas}
}

func (s *Server) putObjects(ctx context.Context, in []api.Object) ([]string, *failure) {
	if len(in) > maxObjects {
		f := tooMany()
		return nil, &f
	}
	objs := make([]gitpack.Object, len(in))
	shas := make([]string, len(in))
	for i, o := range in {
		t := gitpack.ParseType(o.Type)
		if t == 0 {
			f := invalid("ctl: object type %q", o.Type)
			return nil, &f
		}
		data, err := base64.StdEncoding.DecodeString(o.Data)
		if err != nil {
			f := invalid("ctl: data is not base64: %v", err)
			return nil, &f
		}
		if len(data) > gitpack.MaxObjectBytes {
			f := newFailure(CodeObjectTooLarge, fmt.Sprintf("ctl: an object is over %d bytes", gitpack.MaxObjectBytes))
			return nil, &f
		}
		objs[i] = gitpack.Object{Type: t, Data: data}
		shas[i] = gitpack.Hash(t, data).String()
	}
	if len(objs) > 0 {
		if err := s.Store.PutObjects(ctx, objs); err != nil {
			f := storeFail(err)
			return nil, &f
		}
	}
	return shas, nil
}
