package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/majorcontext/motte/api"
	"github.com/majorcontext/motte/client"
	"github.com/majorcontext/motte/internal/gitpack"
	"github.com/majorcontext/motte/internal/vfs/tel"
	volattr "github.com/majorcontext/motte/volume"
)

// ErrGrantRefused is the store answer 403 to a grant: the store holds no read of the repo for the grant, and no
// retry of the call passes.
var ErrGrantRefused = errors.New(volattr.GrantRefused)

// ErrPackTooLarge is the store refusing a pack for its size, or for the work that verifying it takes.
var ErrPackTooLarge = errors.New("vfs: the store refuses the pack as too large")

const (
	maxPutBytes   = 32 << 20
	maxPutObjects = 100000 // the object limit of POST /v1/vcs/objects
)

// HTTPStore reads and writes the version control store through client.Client and adds the spans of the daemon.
type HTTPStore struct {
	BaseURL string
	// Grant returns the bearer grant of each call. A caller whose mount outlives one grant returns a fresh one.
	// A grant that fails fails the call.
	Grant  client.GrantFunc
	Client *http.Client
	// Tel carries the correlation attributes and the logger of the spans and records the client writes.
	Tel tel.Telemetry

	once  sync.Once
	cl    *client.Client
	calls atomic.Int64
}

// Calls is the number of store requests made so far.
func (s *HTTPStore) Calls() int64 { return s.calls.Load() }

type countingTransport struct {
	rt http.RoundTripper
	n  *atomic.Int64
}

type callKey struct{}

// RoundTrip counts the request and, for a request made under begin, records the status and the bytes of the answer on
// the call.
func (c countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	resp, err := c.rt.RoundTrip(r)
	cl, _ := r.Context().Value(callKey{}).(*call)
	if cl == nil || err != nil {
		return resp, err
	}
	cl.status.Store(int64(resp.StatusCode))
	cl.out.Store(max(r.ContentLength, 0))
	resp.Body = &countedBody{ReadCloser: resp.Body, n: &cl.in}
	return resp, nil
}

type countedBody struct {
	io.ReadCloser
	n *atomic.Int64
}

func (b *countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.n.Add(int64(n))
	return n, err
}

// c returns the client. The http.Client timeout bounds a call, so the per-call budget of the client is off.
func (s *HTTPStore) c() *client.Client {
	s.once.Do(func() {
		hc := http.Client{}
		if s.Client != nil {
			hc = *s.Client
		}
		if hc.Transport == nil {
			hc.Transport = http.DefaultTransport
		}
		hc.Transport = countingTransport{hc.Transport, &s.calls}
		s.cl = client.New(s.BaseURL, s.Grant, client.WithHTTPClient(&hc), client.WithBudget(0))
	})
	return s.cl
}

// call is the span of one store request.
type call struct {
	s       *HTTPStore
	span    trace.Span
	route   string
	started time.Time
	// status, in and out are set by the transport: the HTTP status, and the bytes of the answer and of the request.
	status, in, out atomic.Int64
}

// begin starts the span of a request to route and, when a FUSE operation waits on it, links the two.
func (s *HTTPStore) begin(ctx context.Context, name, route string) (context.Context, *call) {
	ctx, span := s.Tel.Start(ctx, name, attribute.String("http.route", route), attribute.String("tier", "store"))
	cl := &call{s: s, span: span, route: route, started: time.Now()}
	return context.WithValue(ctx, callKey{}, cl), cl
}

// end closes the span with the figures of the request, logs a failure once and returns err, with a refused grant
// marked as ErrGrantRefused.
func (c *call) end(ctx context.Context, objects int, err error) error {
	status := int(c.status.Load())
	c.span.SetAttributes(attribute.Int("objects", objects), attribute.Int64("bytes_in", c.in.Load()), attribute.Int64("bytes_out", c.out.Load()),
		attribute.Int("http.response.status_code", status), attribute.Int("retries", 0))
	if ot := tel.OpTraceFrom(ctx); ot != nil {
		ot.AddStore(c.span.SpanContext(), time.Since(c.started))
	}
	if errors.Is(err, client.ErrRepoForbidden) {
		err = fmt.Errorf("%w: %w", ErrGrantRefused, err)
	}
	if err != nil {
		c.s.Tel.LogError(ctx, "vcs.client.error", err, slog.String("route", c.route), slog.Int("http_status", status))
	}
	tel.Finish(c.span, err)
	return err
}

func (s *HTTPStore) Commit(ctx context.Context, sha gitpack.SHA) (gitpack.SHA, time.Time, error) {
	ctx, cl := s.begin(ctx, "vcs.client.commit.get", "/v1/vcs/commits/{sha}")
	c, err := s.c().Commit(ctx, sha.String())
	if err = cl.end(ctx, 1, err); err != nil {
		return gitpack.SHA{}, time.Time{}, err
	}
	tree, err := gitpack.ParseSHA(c.Tree)
	if err != nil {
		return gitpack.SHA{}, time.Time{}, err
	}
	when, err := time.Parse(time.RFC3339, c.Time)
	return tree, when, err
}

func (s *HTTPStore) TreeBulk(ctx context.Context, sha gitpack.SHA, maxEntries int) ([]*Tree, error) {
	ctx, cl := s.begin(ctx, "vcs.client.trees.bulk", "/v1/vcs/trees/{sha}")
	out, err := s.c().Trees(ctx, sha.String(), maxEntries)
	entries := 0
	for _, t := range out.Trees {
		entries += len(t.Entries)
	}
	cl.span.SetAttributes(attribute.Int("trees", len(out.Trees)), attribute.Int("max_entries", maxEntries))
	if err = cl.end(ctx, entries, err); err != nil {
		return nil, err
	}
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

func shaTexts(shas []gitpack.SHA) []string {
	out := make([]string, len(shas))
	for i, h := range shas {
		out[i] = h.String()
	}
	return out
}

func (s *HTTPStore) Blobs(ctx context.Context, shas []gitpack.SHA) ([][]byte, error) {
	ctx, cl := s.begin(ctx, "vcs.client.blobs.batch", "/v1/vcs/blobs/batch")
	blobs, err := s.c().Blobs(ctx, shaTexts(shas))
	if err == nil && len(blobs) != len(shas) {
		err = fmt.Errorf("vfs: store returned %d blobs for %d names", len(blobs), len(shas))
	}
	if err = cl.end(ctx, len(shas), err); err != nil {
		return nil, err
	}
	return blobs, nil
}

// Has reports which of shas the store holds.
func (s *HTTPStore) Has(ctx context.Context, shas []gitpack.SHA) ([]bool, error) {
	ctx, cl := s.begin(ctx, "vcs.client.objects.has", "/v1/vcs/objects/has")
	present, err := s.c().Has(ctx, shaTexts(shas))
	if err == nil && len(present) != len(shas) {
		err = fmt.Errorf("vfs: store answered %d presences for %d names", len(present), len(shas))
	}
	if err = cl.end(ctx, len(shas), err); err != nil {
		return nil, err
	}
	return present, nil
}

// Object reads one object of any type.
func (s *HTTPStore) Object(ctx context.Context, sha gitpack.SHA) (gitpack.Type, []byte, error) {
	ctx, cl := s.begin(ctx, "vcs.client.object.get", "/v1/vcs/objects/{sha}")
	o, err := s.c().Object(ctx, sha.String())
	if err = cl.end(ctx, 1, err); err != nil {
		return 0, nil, err
	}
	t := gitpack.ParseType(o.Type)
	if t == 0 {
		return 0, nil, fmt.Errorf("vfs: store returned object type %q", o.Type)
	}
	return t, o.Data, nil
}

// PutObjects uploads objs in requests of at most maxPutBytes and maxPutObjects. One snapshot is one request unless it is larger.
func (s *HTTPStore) PutObjects(ctx context.Context, objs []gitpack.Object) error {
	for len(objs) > 0 {
		n, size := 0, 0
		for n < len(objs) && n < maxPutObjects && (n == 0 || size+len(objs[n].Data) <= maxPutBytes) {
			size += len(objs[n].Data)
			n++
		}
		batch := make([]client.Object, n)
		for i, o := range objs[:n] {
			batch[i] = client.Object{Type: o.Type.String(), Data: o.Data}
		}
		cctx, cl := s.begin(ctx, "vcs.client.object.put", "/v1/vcs/objects")
		cl.span.SetAttributes(attribute.Int64("raw_bytes", int64(size)))
		_, err := s.c().PutObjects(cctx, batch)
		if err = cl.end(cctx, n, err); err != nil {
			return err
		}
		objs = objs[n:]
	}
	return nil
}

// ImportPack uploads one pack and its v2 index. The pack ID is the SHA-256 of the pack.
func (s *HTTPStore) ImportPack(ctx context.Context, idx, pack []byte) error {
	sum := sha256.Sum256(pack)
	ctx, cl := s.begin(ctx, "vcs.client.pack.import", "/v1/vcs/packs/{pack_id}")
	cl.span.SetAttributes(attribute.Int64("pack_bytes", int64(len(pack))))
	err := cl.end(ctx, 0, s.c().ImportPack(ctx, hex.EncodeToString(sum[:]), idx, pack))
	if errors.Is(err, client.ErrObjectTooLarge) {
		return fmt.Errorf("%w: %w", ErrPackTooLarge, err)
	}
	return err
}

// validName reports whether name is one path component that a checkout can hold.
func validName(name []byte) bool {
	return len(name) > 0 && !bytes.ContainsAny(name, "/\x00") && string(name) != "." && string(name) != ".."
}
