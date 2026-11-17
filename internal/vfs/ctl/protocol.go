// Package ctl serves the control protocol of a workspace: one JSON object for each line over a unix socket. The
// fixtures in testdata/ctl at the repository root pin each operation and its errors.
package ctl

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
)

// Code is the machine-readable cause of an error answer. The store codes of package api pass through, so the
// complete list is these and the codes of api.
type Code string

const (
	CodeInvalidRequest   Code = "invalid_request"
	CodeUnknownOp        Code = "unknown_op"
	CodeLineTooLong      Code = "line_too_long"
	CodeStoreUnavailable Code = "store_unavailable"
	CodeInternal         Code = Code(api.CodeInternal)
	CodeObjectTooLarge   Code = Code(api.CodeObjectTooLarge)
)

type request struct {
	Op          string   `json:"op"`
	Traceparent string   `json:"traceparent"`
	Expect      int64    `json:"expect"`
	Heads       []string `json:"heads"`
	Name        string   `json:"name"`
	Commit      string   `json:"commit"`
	Delete      bool     `json:"delete"`
	Generation  int64    `json:"generation"`

	SHA     string       `json:"sha"`
	Type    string       `json:"type"`
	Data    string       `json:"data"`
	Objects []api.Object `json:"objects"`
	SHAs    []string     `json:"shas"`
	MaxSize int64        `json:"max_size"`
}

// failure is the answer to a request that did not succeed. Ref is the stored ref of a lost ref swap.
type failure struct {
	Err  string   `json:"err"`
	Code Code     `json:"code"`
	Ref  *api.Ref `json:"ref,omitempty"`
}

func invalid(format string, a ...any) failure {
	return failure{Err: fmt.Sprintf(format, a...), Code: CodeInvalidRequest}
}

func internal(err error) failure { return failure{Err: err.Error(), Code: CodeInternal} }

// storeFail answers an error of a store call: the code of the store answer, or store_unavailable when the call
// got no answer.
func storeFail(err error) failure {
	f := failure{Err: err.Error(), Code: CodeStoreUnavailable}
	var ce *client.Error
	if errors.As(err, &ce) && ce.Code != "" {
		f.Code = Code(ce.Code)
	}
	if errors.Is(err, client.ErrGenerationConflict) {
		if ref, ok := client.CurrentRef(err); ok {
			f.Ref = &ref
		}
	}
	return f
}

// Boot names this daemon process. A change token is valid only within one process, so a client that keeps a token
// across calls keeps this value with it.
var bootID = func() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}()

type tokenAnswer struct {
	Boot     string `json:"boot"`
	Token    string `json:"token"`
	BaseTree string `json:"base_tree"`
}

type dirtyAnswer struct {
	tokenAnswer
	Entries []DirtyEntry `json:"entries"`
}

type changesAnswer struct {
	tokenAnswer
	Changes []ChangeEntry `json:"changes"`
}

// DirtyEntry is one path that the upper directory changes against the base tree.
type DirtyEntry struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"` // file, symlink, dir, deleted
	Exec   bool   `json:"exec,omitempty"`
	Opaque bool   `json:"opaque,omitempty"`
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

// Stats are the counters of the node cache.
type Stats struct {
	BlobHits   int64 `json:"blob_hits"`
	BlobMisses int64 `json:"blob_misses"`
	TreeCalls  int64 `json:"tree_calls"`
}

type (
	empty     struct{}
	shaAnswer struct {
		SHA string `json:"sha"`
	}
	shasAnswer struct {
		SHAs []string `json:"shas"`
	}
	objectAnswer struct {
		Type string `json:"type"`
		Data string `json:"data"`
	}
	dataAnswer struct {
		Data string `json:"data"`
	}
	datasAnswer struct {
		Datas []string `json:"datas"`
		Large []int    `json:"large"`
	}
	presentAnswer struct {
		Present []bool `json:"present"`
	}
	opHeadsAnswer struct {
		Gen   int64    `json:"gen"`
		Heads []string `json:"heads"`
	}
	refsAnswer struct {
		Refs []api.Ref `json:"refs"`
	}
	refAnswer struct {
		Ref api.Ref `json:"ref"`
	}
)
