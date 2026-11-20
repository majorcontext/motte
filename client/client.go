// Package client is the typed Go client of the motte store and control planes.
package client

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/propagation"

	"github.com/majorcontext/motte/api"
)

// DefaultBudget is the time limit of one call unless WithBudget sets another.
const DefaultBudget = 2 * time.Second

// DefaultLandBudget is the time limit of a Land call unless WithBudget sets another. It is above the 30 seconds that
// the server waits for a land.
const DefaultLandBudget = 35 * time.Second

const errorBodyMax = 1 << 20

// GrantFunc returns the bearer grant of one call. The client calls it before each request.
type GrantFunc func(ctx context.Context) (string, error)

// Client calls one store. A grant has one scope, so a caller holds one Client
// for each scope it uses. A Client is safe for concurrent use.
type Client struct {
	base   string
	grant  GrantFunc
	http   *http.Client
	budget time.Duration
	// set is true after WithBudget.
	set bool
}

type Option func(*Client)

// WithBudget sets the time limit of one call. Zero or less removes the limit; a deadline on the call context still applies.
func WithBudget(d time.Duration) Option { return func(c *Client) { c.budget, c.set = d, true } }

// WithHTTPClient sets the transport. The client still follows no redirect.
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// New returns a client of the store at baseURL.
func New(baseURL string, grant GrantFunc, opts ...Option) *Client {
	c := &Client{base: strings.TrimRight(baseURL, "/"), grant: grant, http: &http.Client{}, budget: DefaultBudget}
	for _, o := range opts {
		o(c)
	}
	h := *c.http
	h.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.http = &h
	return c
}

// StaticGrant returns a GrantFunc that always answers token.
func StaticGrant(token string) GrantFunc {
	return func(context.Context) (string, error) { return token, nil }
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	return c.doWithin(ctx, c.budget, method, path, in, out)
}

func (c *Client) doWithin(ctx context.Context, budget time.Duration, method, path string, in, out any) error {
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	token, err := c.grant(ctx)
	if err != nil {
		return fmt.Errorf("motte: grant: %w", err)
	}
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return &Error{Status: resp.StatusCode, cause: ErrRedirect}
	case resp.StatusCode >= 400:
		raw, err := io.ReadAll(io.LimitReader(resp.Body, errorBodyMax))
		if err != nil {
			return err
		}
		return decodeError(resp.StatusCode, raw)
	case out == nil || resp.StatusCode == http.StatusNoContent:
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func decodeError(status int, raw []byte) error {
	var body api.ErrorBody
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&body) != nil || body.Error.Code == "" {
		return &Error{Status: status, cause: ErrUnexpected}
	}
	cause, ok := sentinels[body.Error.Code]
	if !ok {
		cause = ErrUnexpected
	}
	return &Error{Status: status, Code: body.Error.Code, Message: body.Error.Message, Details: body.Error.Details, cause: cause}
}

func segs(parts ...string) string {
	out := make([]string, len(parts))
	for i, p := range parts {
		out[i] = url.PathEscape(p)
	}
	return "/" + strings.Join(out, "/")
}

func refPath(name string) string {
	return "/v1/vcs/refs" + segs(strings.Split(name, "/")...)
}

func b64(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

func unb64(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

// Object is one git object. Type is commit, tree, blob or tag.
type Object struct {
	Type string
	Data []byte
}

// Commit returns the root tree and time of a commit.
func (c *Client) Commit(ctx context.Context, sha string) (api.Commit, error) {
	var out api.Commit
	err := c.do(ctx, http.MethodGet, "/v1/vcs/commits"+segs(sha), nil, &out)
	return out, err
}

// Trees returns the tree sha and the trees under it, at most maxEntries entries in all. Zero takes the server default.
func (c *Client) Trees(ctx context.Context, sha string, maxEntries int) (api.Trees, error) {
	path := "/v1/vcs/trees" + segs(sha)
	if maxEntries > 0 {
		path += "?max_entries=" + strconv.Itoa(maxEntries)
	}
	var out api.Trees
	err := c.do(ctx, http.MethodGet, path, nil, &out)
	return out, err
}

// Blobs returns the contents of the blobs, in the order of shas.
func (c *Client) Blobs(ctx context.Context, shas []string) ([][]byte, error) {
	var out api.Blobs
	if err := c.do(ctx, http.MethodPost, "/v1/vcs/blobs/batch", api.BlobsRequest{SHAs: shas}, &out); err != nil {
		return nil, err
	}
	blobs := make([][]byte, len(out.Blobs))
	for i, b := range out.Blobs {
		var err error
		if blobs[i], err = unb64(b); err != nil {
			return nil, err
		}
	}
	return blobs, nil
}

// Object returns the type and content of one object.
func (c *Client) Object(ctx context.Context, sha string) (Object, error) {
	var out api.Object
	if err := c.do(ctx, http.MethodGet, "/v1/vcs/objects"+segs(sha), nil, &out); err != nil {
		return Object{}, err
	}
	data, err := unb64(out.Data)
	return Object{Type: out.Type, Data: data}, err
}

// Has reports for each sha whether the repository holds it.
func (c *Client) Has(ctx context.Context, shas []string) ([]bool, error) {
	var out api.Has
	err := c.do(ctx, http.MethodPost, "/v1/vcs/objects/has", api.HasRequest{SHAs: shas}, &out)
	return out.Present, err
}

// PutObjects stores the objects and returns their names.
func (c *Client) PutObjects(ctx context.Context, objs []Object) ([]string, error) {
	req := api.PutObjectsRequest{Objects: make([]api.Object, len(objs))}
	for i, o := range objs {
		req.Objects[i] = api.Object{Type: o.Type, Data: b64(o.Data)}
	}
	var out api.PutObjectsResponse
	err := c.do(ctx, http.MethodPost, "/v1/vcs/objects", req, &out)
	return out.SHAs, err
}

// ImportPack stores a pack and its v2 index. packID is the lowercase hex SHA-256 of pack.
func (c *Client) ImportPack(ctx context.Context, packID string, index, pack []byte) error {
	return c.do(ctx, http.MethodPut, "/v1/vcs/packs"+segs(packID), api.ImportPackRequest{Index: b64(index), Pack: b64(pack)}, nil)
}

// PutOp stores one op-log object of the workspace. kind is the op kind of the store.
func (c *Client) PutOp(ctx context.Context, kind, id string, data []byte) error {
	return c.do(ctx, http.MethodPut, "/v1/vcs/ops"+segs(kind, id), api.OpObject{Data: b64(data)}, nil)
}

// Op returns one op-log object of the workspace.
func (c *Client) Op(ctx context.Context, kind, id string) ([]byte, error) {
	var out api.OpObject
	if err := c.do(ctx, http.MethodGet, "/v1/vcs/ops"+segs(kind, id), nil, &out); err != nil {
		return nil, err
	}
	return unb64(out.Data)
}

// OpHeads returns the head set of the op log of the workspace.
func (c *Client) OpHeads(ctx context.Context) (api.OpHeads, error) {
	var out api.OpHeads
	err := c.do(ctx, http.MethodGet, "/v1/vcs/op-heads", nil, &out)
	return out, err
}

// SwapOpHeads replaces the head set when the stored generation is expectGeneration. Use 0 for the first write.
func (c *Client) SwapOpHeads(ctx context.Context, expectGeneration int64, heads []string) (api.OpHeads, error) {
	var out api.OpHeads
	err := c.do(ctx, http.MethodPut, "/v1/vcs/op-heads", api.SwapOpHeadsRequest{ExpectGeneration: expectGeneration, Heads: heads}, &out)
	return out, err
}

// SetWorkspace reports the op and commit of the workspace. generation is that of the op-heads swap of op.
func (c *Client) SetWorkspace(ctx context.Context, op, commit string, generation int64) error {
	return c.do(ctx, http.MethodPut, "/v1/vcs/workspace", api.SetWorkspaceRequest{Op: op, Commit: commit, Generation: generation}, nil)
}

// Refs lists the refs of the repository.
func (c *Client) Refs(ctx context.Context) ([]api.Ref, error) {
	var out api.Refs
	err := c.do(ctx, http.MethodGet, "/v1/vcs/refs", nil, &out)
	return out.Refs, err
}

// Ref returns one ref.
func (c *Client) Ref(ctx context.Context, name string) (api.Ref, error) {
	var out api.Ref
	err := c.do(ctx, http.MethodGet, refPath(name), nil, &out)
	return out, err
}

// SwapRef points the ref at commit when its stored generation is expect. Use 0 to create the ref. A conflict
// is ErrGenerationConflict, and CurrentRef returns the stored ref. A client with a merge grant writes any ref of its
// repository with no workspace. A client with a node grant gets ErrRefProtected for a protected ref.
func (c *Client) SwapRef(ctx context.Context, name string, expect int64, commit string) (api.Ref, error) {
	var out api.Ref
	err := c.do(ctx, http.MethodPut, refPath(name), api.SwapRefRequest{Expect: expect, Commit: commit}, &out)
	return out, err
}

// DeleteRef deletes the ref when its stored generation is expect.
func (c *Client) DeleteRef(ctx context.Context, name string, expect int64) error {
	return c.do(ctx, http.MethodDelete, refPath(name)+"?expect="+strconv.FormatInt(expect, 10), nil, nil)
}

// CreateRepo creates a repository with a root commit under main and returns its path.
func (c *Client) CreateRepo(ctx context.Context, name, owner string) (string, error) {
	var out api.CreateRepoResponse
	err := c.do(ctx, http.MethodPost, "/v1/repos", api.CreateRepoRequest{Name: name, Owner: owner}, &out)
	return out.Path, err
}

// Repos lists the repositories, newest first.
func (c *Client) Repos(ctx context.Context) ([]api.Repo, error) {
	var out api.Repos
	err := c.do(ctx, http.MethodGet, "/v1/repos", nil, &out)
	return out.Repos, err
}

// Repo returns one repository.
func (c *Client) Repo(ctx context.Context, path string) (api.Repo, error) {
	var out api.Repo
	err := c.do(ctx, http.MethodGet, "/v1/repos"+segs(path), nil, &out)
	return out, err
}

// CreateUserToken issues a per-user token. The text of the token is in the response and nowhere else.
func (c *Client) CreateUserToken(ctx context.Context, req api.CreateUserTokenRequest) (api.CreateUserTokenResponse, error) {
	var out api.CreateUserTokenResponse
	err := c.do(ctx, http.MethodPost, "/v1/tokens", req, &out)
	return out, err
}

// UserTokens lists the per-user tokens, newest first.
func (c *Client) UserTokens(ctx context.Context) ([]api.UserToken, error) {
	var out api.UserTokens
	err := c.do(ctx, http.MethodGet, "/v1/tokens", nil, &out)
	return out.Tokens, err
}

// RevokeUserToken revokes the token with id.
func (c *Client) RevokeUserToken(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/tokens"+segs(id), nil, nil)
}

// Claim binds or raises the epoch of a workspace and returns its stored state.
func (c *Client) Claim(ctx context.Context, workspace string, req api.ClaimRequest) (api.ClaimResponse, error) {
	var out api.ClaimResponse
	err := c.do(ctx, http.MethodPut, "/v1/workspaces"+segs(workspace, "claim"), req, &out)
	return out, err
}

// Flush stores the state of a workspace under its claim epoch.
func (c *Client) Flush(ctx context.Context, workspace string, req api.FlushRequest) error {
	return c.do(ctx, http.MethodPost, "/v1/workspaces"+segs(workspace, "flush"), req, nil)
}

// DeleteWorkspace deletes a workspace. It is idempotent.
func (c *Client) DeleteWorkspace(ctx context.Context, workspace string) error {
	return c.do(ctx, http.MethodDelete, "/v1/workspaces"+segs(workspace), nil, nil)
}

// SetProtectedRefs replaces the protected ref patterns of repo, the repository text. A pattern is a ref name, or a
// prefix that ends in "*". An empty list protects nothing.
func (c *Client) SetProtectedRefs(ctx context.Context, repo string, patterns []string) error {
	return c.do(ctx, http.MethodPut, "/v1/repos"+segs(repo, "protected-refs"), api.ProtectedRefs{Patterns: patterns}, nil)
}

// ProtectedRefs returns the protected ref patterns of repo, the repository text.
func (c *Client) ProtectedRefs(ctx context.Context, repo string) ([]string, error) {
	var out api.ProtectedRefs
	err := c.do(ctx, http.MethodGet, "/v1/repos"+segs(repo, "protected-refs"), nil, &out)
	return out.Patterns, err
}

// Land lands the commits base..head of req on the ref of req. repo is the repository text of the grant, and the grant
// has the merge scope, or the write scope for an unprotected ref. The call waits for the result, at most
// DefaultLandBudget unless WithBudget sets another. A conflict is ErrLandConflict and ConflictPaths returns its paths.
// A land whose tree is not req.ExpectTree is ErrLandStale and StaleTree returns the tree that it would have made.
// An invalid range is ErrLandInvalid, a land that the server did not end in time is ErrLandTimeout, and a repeat of
// the same ID then returns the result once it exists.
func (c *Client) Land(ctx context.Context, repo string, req api.LandRequest) (api.LandResponse, error) {
	budget := c.budget
	if !c.set {
		budget = DefaultLandBudget
	}
	var out api.LandResponse
	err := c.doWithin(ctx, budget, http.MethodPost, "/v1/repos"+segs(repo, "lands"), req, &out)
	return out, err
}

// Events returns up to limit ref events after cursor, and the cursor to pass next. An empty cursor starts at the
// first event. Zero limit takes the server default. Events of one ref arrive in order. ErrEventsPruned means
// retention removed events after the cursor, and the caller must read the refs again.
func (c *Client) Events(ctx context.Context, cursor string, limit int) ([]api.Event, string, error) {
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	var out api.Events
	err := c.do(ctx, http.MethodGet, "/v1/events?"+q.Encode(), nil, &out)
	return out.Events, out.Cursor, err
}

// CreateSubscription registers a receiver of ref events. Its cursor starts at the newest event.
func (c *Client) CreateSubscription(ctx context.Context, req api.CreateSubscriptionRequest) (api.Subscription, error) {
	var out api.Subscription
	err := c.do(ctx, http.MethodPost, "/v1/subscriptions", req, &out)
	return out, err
}

// Subscriptions lists the registered receivers.
func (c *Client) Subscriptions(ctx context.Context) ([]api.Subscription, error) {
	var out api.Subscriptions
	err := c.do(ctx, http.MethodGet, "/v1/subscriptions", nil, &out)
	return out.Subscriptions, err
}

// DeleteSubscription removes a receiver. An unknown id is ErrNotFound.
func (c *Client) DeleteSubscription(ctx context.Context, id string) error {
	return c.do(ctx, http.MethodDelete, "/v1/subscriptions"+segs(id), nil, nil)
}
