//go:build linux

package vfs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/gitpack"
)

func ctlRefs(ctx context.Context, s RefStore, rq ctlReq) ctlResp {
	refs, ref, err := ServeRefs(ctx, s, rq.Op, rq.Name, rq.Expect, rq.Commit, rq.Delete)
	var conflict *RefConflictError
	if errors.As(err, &conflict) {
		return ctlResp{Err: err.Error(), Code: conflictCode, Ref: conflict.Ref}
	}
	if err != nil {
		return ctlResp{Err: err.Error()}
	}
	return ctlResp{Refs: refs, Ref: ref}
}

const maxCtlObjects = 4096

// ctlHas answers has: whether the store holds each object, in request order.
func ctlHas(ctx context.Context, s HasStore, names []string) ctlResp {
	if len(names) > maxCtlObjects {
		return ctlResp{Err: fmt.Sprintf("vfs: more than %d objects in one request", maxCtlObjects)}
	}
	shas := make([]gitpack.SHA, len(names))
	for i, n := range names {
		var err error
		if shas[i], err = gitpack.ParseSHA(n); err != nil {
			return ctlResp{Err: err.Error()}
		}
	}
	present, err := s.Has(ctx, shas)
	if err != nil {
		return ctlResp{Err: err.Error()}
	}
	return ctlResp{Present: present}
}

// ctlWorkspace answers workspace: rq.SHA is the head operation, rq.Commit the commit of the mount and rq.Generation
// the op-heads generation of the swap that set the operation.
func ctlWorkspace(ctx context.Context, s WorkspaceStore, rq ctlReq) ctlResp {
	ctx, cancel := context.WithTimeout(ctx, opHeadsBudget)
	defer cancel()
	if err := s.PutWorkspace(ctx, rq.SHA, rq.Commit, rq.Generation); err != nil {
		return ctlResp{Err: err.Error()}
	}
	return ctlResp{}
}

// ctlOp stores or reads one object of the op log: Type is the kind ("operations" or "views") and SHA the ID.
func ctlOp(ctx context.Context, s OpObjectStore, rq ctlReq) ctlResp {
	if rq.Op == "getop" {
		data, err := s.GetOp(ctx, rq.Type, rq.SHA)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		return ctlResp{Data: base64.StdEncoding.EncodeToString(data)}
	}
	data, err := base64.StdEncoding.DecodeString(rq.Data)
	if err == nil {
		err = s.PutOp(ctx, rq.Type, rq.SHA, data)
	}
	if err != nil {
		return ctlResp{Err: err.Error()}
	}
	return ctlResp{}
}

// ctlObjects serves the object operations of the jj shims: get, put and putbatch. The names of put objects are
// computed here, so a client cannot name an object it did not write.
func (v *VFS) ctlObjects(ctx context.Context, rq ctlReq) ctlResp {
	switch rq.Op {
	case "get":
		sha, err := gitpack.ParseSHA(rq.SHA)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		t, data, err := v.cfg.Store.Object(ctx, sha)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		return ctlResp{Type: t.String(), Data: base64.StdEncoding.EncodeToString(data)}
	case "has":
		if s, ok := v.cfg.Store.(HasStore); ok {
			return ctlHas(ctx, s, rq.SHAs)
		}
	case "workspace":
		if s, ok := v.cfg.Store.(WorkspaceStore); ok {
			return ctlWorkspace(ctx, s, rq)
		}
	case "put":
		shas, err := v.putObjects(ctx, []api.Object{{Type: rq.Type, Data: rq.Data}})
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		return ctlResp{SHA: shas[0]}
	case "putbatch":
		shas, err := v.putObjects(ctx, rq.Objects)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		return ctlResp{SHAs: shas}
	}
	return ctlResp{Err: unknownOp}
}

func (v *VFS) putObjects(ctx context.Context, in []api.Object) ([]string, error) {
	if len(in) > maxCtlObjects {
		return nil, fmt.Errorf("vfs: more than %d objects in one request", maxCtlObjects)
	}
	objs := make([]gitpack.Object, len(in))
	shas := make([]string, len(in))
	for i, o := range in {
		t := gitpack.ParseType(o.Type)
		if t == 0 {
			return nil, fmt.Errorf("vfs: object type %q", o.Type)
		}
		data, err := base64.StdEncoding.DecodeString(o.Data)
		if err != nil {
			return nil, err
		}
		objs[i] = gitpack.Object{Type: t, Data: data}
		shas[i] = gitpack.Hash(t, data).String()
	}
	if len(objs) > 0 {
		if err := v.cfg.Store.PutObjects(ctx, objs); err != nil {
			return nil, err
		}
	}
	return shas, nil
}
