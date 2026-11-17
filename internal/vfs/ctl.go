//go:build linux

package vfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/majorcontext/motte/api"
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

// handleCtl serves the control protocol of the workspace: the dirty set, the change token and the store operations.
func (v *VFS) handleCtl(ctx context.Context, rq ctlReq) ctlResp {
	ctx = WithTraceparent(ctx, rq.Traceparent)
	switch rq.Op {
	case "token":
		tok, err := upperdir.Fingerprint(v.upperFS, "", v.now())
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		return v.upperResp(tok)
	case "dirty":
		es, tok, err := upperdir.ScanStable(ctx, v.upperFS, "", UpperBase(v.c, v.base), v.now)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		rs := v.upperResp(tok)
		rs.Entries = DirtyFromUpper(es)
		return rs
	case "changes":
		cs, tok, err := v.upperChanges(ctx)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		rs := v.upperResp(tok)
		rs.Changes = cs
		return rs
	case "getbatch":
		return v.getBlobs(ctx, rq.SHAs, rq.MaxSize)
	case "stats":
		return ctlResp{Stats: v.Stats()}
	case "opheads":
		s, ok := v.cfg.Store.(OpHeadsStore)
		if !ok {
			break
		}
		h, err := ServeOpHeads(ctx, s, rq.Expect, rq.Heads)
		if err != nil {
			return ctlResp{Err: err.Error()}
		}
		return ctlResp{Gen: uint64(h.Generation), Heads: h.Heads}
	case "refs", "swapref":
		s, ok := v.cfg.Store.(RefStore)
		if !ok {
			break
		}
		return ctlRefs(ctx, s, rq)
	case "putop", "getop":
		s, ok := v.cfg.Store.(OpObjectStore)
		if !ok {
			break
		}
		return ctlOp(ctx, s, rq)
	}
	return v.ctlObjects(ctx, rq)
}

// DirtyEntry is one path that the upper directory changes against the base tree.
type DirtyEntry struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"` // file, symlink, dir, deleted
	Exec   bool   `json:"exec,omitempty"`
	Opaque bool   `json:"opaque,omitempty"`
}

func (v *VFS) Stats() string {
	return fmt.Sprintf("blob_hits=%d blob_misses=%d tree_calls=%d", v.c.BlobHits.Load(), v.c.BlobMisses.Load(), v.c.TreeMisses.Load())
}
