// Package api holds the wire types and error codes of the /v1 routes.
package api

import "net/http"

// Code is a wire error code.
type Code string

const (
	CodeNotFound           Code = "vcs_not_found"
	CodeGenerationConflict Code = "vcs_generation_conflict"
	CodeClaimLost          Code = "vcs_claim_lost"
	CodeObjectInvalid      Code = "vcs_object_invalid"
	CodeObjectTooLarge     Code = "vcs_object_too_large"
	CodeRepoForbidden      Code = "vcs_repo_forbidden"
	CodeWorkspaceConflict  Code = "vcs_workspace_conflict"
	CodeWorkspaceDeleted   Code = "vcs_workspace_deleted"
	CodeRefProtected       Code = "vcs_ref_protected"
	CodeEventsPruned       Code = "vcs_events_pruned"
	CodeUnauthorized       Code = "unauthorized"
	CodeInvalidBody        Code = "invalid_body"
	CodeInternal           Code = "internal_error"
	CodeMethodNotAllowed   Code = "method_not_allowed"
)

var statuses = map[Code]int{
	CodeNotFound:           http.StatusNotFound,
	CodeGenerationConflict: http.StatusConflict,
	CodeClaimLost:          http.StatusConflict,
	CodeObjectInvalid:      http.StatusBadRequest,
	CodeObjectTooLarge:     http.StatusRequestEntityTooLarge,
	CodeRepoForbidden:      http.StatusForbidden,
	CodeWorkspaceConflict:  http.StatusConflict,
	CodeWorkspaceDeleted:   http.StatusGone,
	CodeRefProtected:       http.StatusForbidden,
	CodeEventsPruned:       http.StatusGone,
	CodeUnauthorized:       http.StatusUnauthorized,
	CodeInvalidBody:        http.StatusBadRequest,
	CodeInternal:           http.StatusInternalServerError,
	CodeMethodNotAllowed:   http.StatusMethodNotAllowed,
}

// Status is the HTTP status of c. An unknown code is 500.
func (c Code) Status() int {
	if s, ok := statuses[c]; ok {
		return s
	}
	return http.StatusInternalServerError
}

// ErrorBody is the body of every error response. A conflict on a ref sets
// Details["ref"] to the stored Ref.
type ErrorBody struct {
	Error Error `json:"error"`
}

type Error struct {
	Code    Code           `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }

// OpHeads is the head set of the op log of a workspace. Generation starts at 1.
type OpHeads struct {
	Epoch      uint64   `json:"epoch"`
	Generation int64    `json:"generation"`
	Heads      []string `json:"heads"`
}

// SwapOpHeadsRequest is the body of PUT /v1/vcs/op-heads. ExpectGeneration is
// 0 for the first write.
type SwapOpHeadsRequest struct {
	ExpectGeneration int64    `json:"expect_generation"`
	Heads            []string `json:"heads"`
}

// SetWorkspaceRequest is the body of PUT /v1/vcs/workspace. Generation is that
// of the op-heads swap of Op.
type SetWorkspaceRequest struct {
	Op         string `json:"op"`
	Commit     string `json:"commit"`
	Generation int64  `json:"generation"`
}

// Ref is one ref of a repository. Commit is 40 lowercase hex digits.
// Generation is never reused, even when the ref is deleted and created again.
type Ref struct {
	Name       string `json:"name"`
	Commit     string `json:"commit"`
	Generation int64  `json:"generation"`
}

type Refs struct {
	Refs []Ref `json:"refs"`
}

// SwapRefRequest is the body of PUT /v1/vcs/refs/{name}. Expect is the stored
// generation, or 0 to create the ref.
type SwapRefRequest struct {
	Expect int64  `json:"expect"`
	Commit string `json:"commit"`
}

type OpObject struct {
	Data string `json:"data"`
}

// Commit is the root tree of a commit and its committer time (RFC 3339).
type Commit struct {
	Tree string `json:"tree"`
	Time string `json:"time"`
}

// TreeEntry is one entry of a tree. Name is the base64 of the name bytes,
// which need not be UTF-8. Mode is the octal git mode.
type TreeEntry struct {
	Name string `json:"name"`
	Mode string `json:"mode"`
	SHA  string `json:"sha"`
	Size int64  `json:"size,omitempty"`
}

type Tree struct {
	SHA     string      `json:"sha"`
	Entries []TreeEntry `json:"entries"`
}

type Trees struct {
	Trees []Tree `json:"trees"`
}

type BlobsRequest struct {
	SHAs []string `json:"shas"`
}

type Blobs struct {
	Blobs []string `json:"blobs"`
}

type HasRequest struct {
	SHAs []string `json:"shas"`
}

type Has struct {
	Present []bool `json:"present"`
}

// Object is one git object. Type is commit, tree, blob or tag. Data is base64.
type Object struct {
	Type string `json:"type"`
	Data string `json:"data"`
}

type PutObjectsRequest struct {
	Objects []Object `json:"objects"`
}

type PutObjectsResponse struct {
	SHAs []string `json:"shas"`
}

// ImportPackRequest is the body of PUT /v1/vcs/packs/{pack_id}. The pack ID is
// the lowercase hex SHA-256 of the pack bytes. Both fields are base64.
type ImportPackRequest struct {
	Index string `json:"index"`
	Pack  string `json:"pack"`
}

// CreateRepoRequest is the body of POST /v1/repos.
type CreateRepoRequest struct {
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

type CreateRepoResponse struct {
	Path string `json:"path"`
}

// Repo is one repository of the store.
type Repo struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	Owner     string `json:"owner"`
	CreatedAt string `json:"created_at"`
}

type Repos struct {
	Repos []Repo `json:"repos"`
}

// ClaimRequest is the body of PUT /v1/workspaces/{id}/claim.
type ClaimRequest struct {
	Epoch uint64 `json:"epoch"`
	Owner string `json:"owner"`
	Repo  string `json:"repo"`
}

// ClaimResponse returns the stored epoch, op and snapshot of the workspace.
type ClaimResponse struct {
	Epoch    uint64 `json:"epoch"`
	Op       string `json:"op"`
	Snapshot string `json:"snapshot"`
}

// FlushRequest is the body of POST /v1/workspaces/{id}/flush.
type FlushRequest struct {
	Epoch      uint64 `json:"epoch"`
	Generation int64  `json:"generation"`
	Prev       string `json:"prev"`
	Op         string `json:"op"`
	Snapshot   string `json:"snapshot"`
}

// ProtectedRefs is the body of PUT and GET /v1/repos/{repo}/protected-refs. A
// pattern is a ref name, or a prefix that ends in "*".
type ProtectedRefs struct {
	Patterns []string `json:"patterns"`
}

// Event is one change of a ref. Its identity is Partition and Seq. Events of
// one ref share a partition, and Seq has no gap and follows commit order in a
// partition. No order holds across partitions. Old is empty for a create and
// New is empty for a delete. Generation is that of the ref after the change, or
// of the deleted ref. Kind is the scope of the grant that wrote the ref: node,
// write, merge or operator. Time is RFC 3339.
type Event struct {
	Partition  int    `json:"partition"`
	Seq        int64  `json:"seq"`
	Repo       string `json:"repo"`
	Ref        string `json:"ref"`
	Old        string `json:"old"`
	New        string `json:"new"`
	Generation int64  `json:"generation"`
	Kind       string `json:"kind"`
	Time       string `json:"time"`
}

// Events is the answer of GET /v1/events. Cursor is opaque. A consumer passes it
// as the cursor of the next call, and gets the events after the last one here.
type Events struct {
	Events []Event `json:"events"`
	Cursor string  `json:"cursor"`
}

// CreateSubscriptionRequest is the body of POST /v1/subscriptions. Repo is empty
// for the events of every repo.
type CreateSubscriptionRequest struct {
	URL    string `json:"url"`
	Secret string `json:"secret"`
	Repo   string `json:"repo,omitempty"`
}

// Subscription is a registered receiver. Cursor is an event cursor that holds
// the last event that it acknowledged in each partition. The secret is never returned.
type Subscription struct {
	ID        string `json:"id"`
	URL       string `json:"url"`
	Repo      string `json:"repo,omitempty"`
	Cursor    string `json:"cursor"`
	CreatedAt string `json:"created_at"`
}

type Subscriptions struct {
	Subscriptions []Subscription `json:"subscriptions"`
}

// Headers of an event delivery. Signature is "sha256=" and the lowercase hex
// HMAC-SHA256 of the request body under the secret of the subscription.
const (
	HeaderSignature = "X-Motte-Signature"
	HeaderPartition = "X-Motte-Partition"
	HeaderSequence  = "X-Motte-Sequence"
)
