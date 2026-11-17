package ctl

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/vfs/store"
	"github.com/majorcontext/motte/internal/vfs/tel"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
)

type ctlReq struct {
	Op          string   `json:"op"`
	Traceparent string   `json:"traceparent,omitempty"`
	Expect      int64    `json:"expect,omitempty"`
	Heads       []string `json:"heads,omitempty"`
	Name        string   `json:"name,omitempty"`
	Commit      string   `json:"commit,omitempty"`
	Delete      bool     `json:"delete,omitempty"`
	Generation  int64    `json:"generation,omitempty"`

	SHA     string       `json:"sha,omitempty"`
	Type    string       `json:"type,omitempty"`
	Data    string       `json:"data,omitempty"`
	Objects []api.Object `json:"objects,omitempty"`
	SHAs    []string     `json:"shas,omitempty"`
	MaxSize int64        `json:"max_size,omitempty"`
}

type ctlResp struct {
	Boot     string       `json:"boot,omitempty"`
	Err      string       `json:"err,omitempty"`
	Gen      uint64       `json:"gen,omitempty"`
	Token    string       `json:"token,omitempty"`
	BaseTree string       `json:"base_tree,omitempty"`
	Entries  []DirtyEntry `json:"entries,omitempty"`
	Stats    string       `json:"stats,omitempty"`

	SHA   string   `json:"sha,omitempty"`
	SHAs  []string `json:"shas,omitempty"`
	Type  string   `json:"type,omitempty"`
	Data  string   `json:"data,omitempty"`
	Heads []string `json:"heads,omitempty"`

	Refs []api.Ref `json:"refs,omitempty"`
	Ref  *api.Ref  `json:"ref,omitempty"`
	Code string    `json:"code,omitempty"`

	Changes []ChangeEntry `json:"changes,omitempty"`
	Datas   []string      `json:"datas,omitempty"`
	Large   []int         `json:"large,omitempty"`

	Present []bool `json:"present,omitempty"`
}

const unknownOp = "unknown op"

// bootID names this daemon process. A change token is valid only within one process, so a client that keeps a
// token across calls keeps this value with it.
var bootID = func() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}()

// Workspace is the part of the protocol that the lower layer answers: the change token, the dirty set and the changes
// of the upper directory against the base tree.
type Workspace interface {
	BaseTree() string
	Token() (upperdir.Token, error)
	Dirty(ctx context.Context) ([]DirtyEntry, upperdir.Token, error)
	Changes(ctx context.Context) ([]ChangeEntry, upperdir.Token, error)
	Stats() string
}

// Server serves the control protocol of one workspace.
type Server struct {
	Store     store.Store
	Cache     *store.Cache
	Workspace Workspace
}

func (s *Server) upperResp(tok upperdir.Token) ctlResp {
	return ctlResp{Boot: bootID, Token: tok.String(bootID), BaseTree: s.Workspace.BaseTree()}
}

// handle serves one request: the dirty set, the change token and the store operations.
func (s *Server) handle(ctx context.Context, rq ctlReq) ctlResp {
	ctx = tel.WithTraceparent(ctx, rq.Traceparent)
	switch rq.Op {
	case "token":
		tok, err := s.Workspace.Token()
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		return s.upperResp(tok)
	case "dirty":
		es, tok, err := s.Workspace.Dirty(ctx)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		rs := s.upperResp(tok)
		rs.Entries = es
		return rs
	case "changes":
		cs, tok, err := s.Workspace.Changes(ctx)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		rs := s.upperResp(tok)
		rs.Changes = cs
		return rs
	case "getbatch":
		return s.getBlobs(ctx, rq.SHAs, rq.MaxSize)
	case "stats":
		return ctlResp{Stats: s.Workspace.Stats()}
	case "opheads":
		st, ok := s.Store.(store.OpHeadsStore)
		if !ok {
			break
		}
		h, err := ServeOpHeads(ctx, st, rq.Expect, rq.Heads)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		return ctlResp{Gen: uint64(h.Generation), Heads: h.Heads}
	case "refs", "swapref":
		st, ok := s.Store.(store.RefStore)
		if !ok {
			break
		}
		return ctlRefs(ctx, st, rq)
	case "putop", "getop":
		st, ok := s.Store.(store.OpObjectStore)
		if !ok {
			break
		}
		return ctlOp(ctx, st, rq)
	}
	return s.ctlObjects(ctx, rq)
}

// ChangeEntry is one file whose content in the view differs from the base tree.
// The SHAs are git blob names; an absent side has no SHA and mode 0.
type ChangeEntry struct {
	Path    string `json:"path"`
	Status  string `json:"status"`
	OldSHA  string `json:"old_sha,omitempty"`
	OldMode uint32 `json:"old_mode,omitempty"`
	OldSize int64  `json:"old_size,omitempty"`
	NewSHA  string `json:"new_sha,omitempty"`
	NewMode uint32 `json:"new_mode,omitempty"`
	Size    int64  `json:"size,omitempty"`
}

// DirtyEntry is one path that the upper directory changes against the base tree.
type DirtyEntry struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"` // file, symlink, dir, deleted
	Exec   bool   `json:"exec,omitempty"`
	Opaque bool   `json:"opaque,omitempty"`
}
