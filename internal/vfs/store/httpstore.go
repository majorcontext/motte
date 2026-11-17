package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/tel"
	volattr "github.com/majorcontext/motte/volume"
)

// ErrGrantRefused is the store answer 403 to a grant: the store holds no read of the repo for the grant, and no
// retry of the call passes.
var ErrGrantRefused = errors.New(volattr.GrantRefused)

const (
	maxPutBytes   = 32 << 20
	maxPutObjects = 100000 // the object limit of POST /v1/vcs/objects
)

// HTTPStore reads and writes the version control store over its /v1/vcs routes.
type HTTPStore struct {
	BaseURL string
	// Grant returns the bearer grant of each call. A caller whose mount outlives one grant returns a fresh one.
	Grant  func() (string, error)
	Client *http.Client
	// Tel carries the correlation attributes and the logger of the spans and records the client writes.
	Tel tel.Telemetry

	calls atomic.Int64
}

// Calls is the number of store requests made so far.
func (s *HTTPStore) Calls() int64 { return s.calls.Load() }

// call is the span of one store request.
type call struct {
	s       *HTTPStore
	span    trace.Span
	route   string
	started time.Time
	status  int
	in, out int64
}

// begin starts the span of a request to route and, when a FUSE operation waits on it, links the two.
func (s *HTTPStore) begin(ctx context.Context, name, route string) (context.Context, *call) {
	ctx, span := s.Tel.Start(ctx, name, attribute.String("http.route", route), attribute.String("tier", "store"))
	return ctx, &call{s: s, span: span, route: route, started: time.Now()}
}

// end closes the span with the figures of the request and logs a failure once.
func (c *call) end(ctx context.Context, objects int, err error) {
	c.span.SetAttributes(attribute.Int("objects", objects), attribute.Int64("bytes_in", c.in), attribute.Int64("bytes_out", c.out),
		attribute.Int("http.response.status_code", c.status), attribute.Int("retries", 0))
	if ot := tel.OpTraceFrom(ctx); ot != nil {
		ot.AddStore(c.span.SpanContext(), time.Since(c.started))
	}
	if err != nil {
		c.s.Tel.LogError(ctx, "vcs.client.error", err, slog.String("route", c.route), slog.Int("http_status", c.status))
	}
	tel.Finish(c.span, err)
}

func (s *HTTPStore) do(ctx context.Context, c *call, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		c.out = int64(len(b))
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.BaseURL+path, rd)
	if err != nil {
		return err
	}
	grant, err := s.Grant()
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+grant)
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(req.Header))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	hc := s.Client
	if hc == nil {
		hc = http.DefaultClient
	}
	noRedirect := *hc
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	hc = &noRedirect
	s.calls.Add(1)
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	c.status = resp.StatusCode
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
		msg := bytes.TrimSpace(b[:min(len(b), 1<<10)])
		if resp.StatusCode == http.StatusForbidden {
			return fmt.Errorf("%w: %s %s: %s", ErrGrantRefused, method, path, msg)
		}
		return &statusError{error: fmt.Errorf("vfs: %s %s: %s: %s", method, path, resp.Status, msg), status: resp.StatusCode, body: b}
	}
	if out == nil {
		return nil
	}
	cr := &countingReader{r: resp.Body}
	err = json.NewDecoder(cr).Decode(out)
	c.in = cr.n
	return err
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (s *HTTPStore) Commit(ctx context.Context, sha gitpack.SHA) (gitpack.SHA, time.Time, error) {
	ctx, cl := s.begin(ctx, "vcs.client.commit.get", "/v1/vcs/commits/{sha}")
	var c api.Commit
	if err := s.do(ctx, cl, http.MethodGet, "/v1/vcs/commits/"+sha.String(), nil, &c); err != nil {
		cl.end(ctx, 0, err)
		return gitpack.SHA{}, time.Time{}, err
	}
	cl.end(ctx, 1, nil)
	tree, err := gitpack.ParseSHA(c.Tree)
	if err != nil {
		return gitpack.SHA{}, time.Time{}, err
	}
	when, err := time.Parse(time.RFC3339, c.Time)
	return tree, when, err
}

func (s *HTTPStore) TreeBulk(ctx context.Context, sha gitpack.SHA, maxEntries int) ([]*Tree, error) {
	ctx, cl := s.begin(ctx, "vcs.client.trees.bulk", "/v1/vcs/trees/{sha}")
	q := url.Values{"max_entries": {strconv.Itoa(maxEntries)}}
	var out api.Trees
	if err := s.do(ctx, cl, http.MethodGet, "/v1/vcs/trees/"+sha.String()+"?"+q.Encode(), nil, &out); err != nil {
		cl.end(ctx, 0, err)
		return nil, err
	}
	entries := 0
	for _, t := range out.Trees {
		entries += len(t.Entries)
	}
	cl.span.SetAttributes(attribute.Int("trees", len(out.Trees)), attribute.Int("max_entries", maxEntries))
	cl.end(ctx, entries, nil)
	trees := make([]*Tree, 0, len(out.Trees))
	for _, vt := range out.Trees {
		t, err := decodeTree(vt)
		if err != nil {
			return nil, err
		}
		trees = append(trees, t)
	}
	return trees, nil
}

func decodeTree(vt api.Tree) (*Tree, error) {
	sha, err := gitpack.ParseSHA(vt.SHA)
	if err != nil {
		return nil, err
	}
	es := make([]Entry, len(vt.Entries))
	for i, ve := range vt.Entries {
		name, err := base64.StdEncoding.DecodeString(ve.Name)
		if err != nil {
			return nil, err
		}
		if !validName(name) {
			return nil, fmt.Errorf("vfs: tree %s holds the entry name %q", vt.SHA, name)
		}
		mode, err := strconv.ParseUint(ve.Mode, 8, 32)
		if err != nil {
			return nil, err
		}
		es[i] = Entry{Name: string(name), Mode: uint32(mode), Size: ve.Size}
		if es[i].SHA, err = gitpack.ParseSHA(ve.SHA); err != nil {
			return nil, err
		}
	}
	return NewTree(sha, es), nil
}

func (s *HTTPStore) Blobs(ctx context.Context, shas []gitpack.SHA) ([][]byte, error) {
	req := api.BlobsRequest{SHAs: make([]string, len(shas))}
	for i, h := range shas {
		req.SHAs[i] = h.String()
	}
	ctx, cl := s.begin(ctx, "vcs.client.blobs.batch", "/v1/vcs/blobs/batch")
	var out api.Blobs
	err := s.do(ctx, cl, http.MethodPost, "/v1/vcs/blobs/batch", req, &out)
	if err == nil && len(out.Blobs) != len(shas) {
		err = fmt.Errorf("vfs: store returned %d blobs for %d names", len(out.Blobs), len(shas))
	}
	cl.end(ctx, len(shas), err)
	if err != nil {
		return nil, err
	}
	blobs := make([][]byte, len(shas))
	for i, b := range out.Blobs {
		var err error
		if blobs[i], err = base64.StdEncoding.DecodeString(b); err != nil {
			return nil, err
		}
	}
	return blobs, nil
}

// Has reports which of shas the store holds.
func (s *HTTPStore) Has(ctx context.Context, shas []gitpack.SHA) ([]bool, error) {
	req := api.HasRequest{SHAs: make([]string, len(shas))}
	for i, h := range shas {
		req.SHAs[i] = h.String()
	}
	ctx, cl := s.begin(ctx, "vcs.client.objects.has", "/v1/vcs/objects/has")
	var out api.Has
	err := s.do(ctx, cl, http.MethodPost, "/v1/vcs/objects/has", req, &out)
	if err == nil && len(out.Present) != len(shas) {
		err = fmt.Errorf("vfs: store answered %d presences for %d names", len(out.Present), len(shas))
	}
	cl.end(ctx, len(shas), err)
	if err != nil {
		return nil, err
	}
	return out.Present, nil
}

// Object reads one object of any type.
func (s *HTTPStore) Object(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	ctx, cl := s.begin(ctx, "vcs.client.object.get", "/v1/vcs/objects/{sha}")
	var o api.Object
	err := s.do(ctx, cl, http.MethodGet, "/v1/vcs/objects/"+sha.String(), nil, &o)
	cl.end(ctx, 1, err)
	if err != nil {
		return 0, nil, err
	}
	data, err := base64.StdEncoding.DecodeString(o.Data)
	if err != nil {
		return 0, nil, err
	}
	t := gitpack.ParseType(o.Type)
	if t == 0 {
		return 0, nil, fmt.Errorf("vfs: store returned object type %q", o.Type)
	}
	return t, data, nil
}

// PutObjects uploads objs in requests of at most maxPutBytes and maxPutObjects. One snapshot is one request unless it is larger.
func (s *HTTPStore) PutObjects(ctx context.Context, objs []gitpack.Object) error {
	for len(objs) > 0 {
		n, size := 0, 0
		for n < len(objs) && n < maxPutObjects && (n == 0 || size+len(objs[n].Data) <= maxPutBytes) {
			size += len(objs[n].Data)
			n++
		}
		req := api.PutObjectsRequest{Objects: make([]api.Object, n)}
		for i, o := range objs[:n] {
			req.Objects[i] = api.Object{Type: o.Type.String(), Data: base64.StdEncoding.EncodeToString(o.Data)}
		}
		cctx, cl := s.begin(ctx, "vcs.client.object.put", "/v1/vcs/objects")
		cl.span.SetAttributes(attribute.Int64("raw_bytes", int64(size)))
		err := s.do(cctx, cl, http.MethodPost, "/v1/vcs/objects", req, nil)
		cl.end(cctx, n, err)
		if err != nil {
			return err
		}
		objs = objs[n:]
	}
	return nil
}

// validName reports whether name is one path component that a checkout can hold.
func validName(name []byte) bool {
	return len(name) > 0 && !bytes.ContainsAny(name, "/\x00") && string(name) != "." && string(name) != ".."
}

// ImportPack uploads one pack and its v2 index. The pack ID is the SHA-256 of the pack.
func (s *HTTPStore) ImportPack(ctx context.Context, idx, pack []byte) error {
	sum := sha256.Sum256(pack)
	req := api.ImportPackRequest{Index: base64.StdEncoding.EncodeToString(idx), Pack: base64.StdEncoding.EncodeToString(pack)}
	ctx, cl := s.begin(ctx, "vcs.client.pack.import", "/v1/vcs/packs/{pack_id}")
	cl.span.SetAttributes(attribute.Int64("pack_bytes", int64(len(pack))))
	err := s.do(ctx, cl, http.MethodPut, "/v1/vcs/packs/"+hex.EncodeToString(sum[:]), req, nil)
	cl.end(ctx, 0, err)
	return packTooLarge(err)
}

// ErrPackTooLarge is the store refusing a pack for its size, or for the work that verifying it takes.
var ErrPackTooLarge = errors.New("vfs: the store refuses the pack as too large")

func packTooLarge(err error) error {
	var se *statusError
	if !errors.As(err, &se) {
		return err
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if json.Unmarshal(se.body, &body) != nil || body.Error.Code != "vcs_object_too_large" {
		return err
	}
	return fmt.Errorf("%w: %w", ErrPackTooLarge, err)
}
