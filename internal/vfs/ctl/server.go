package ctl

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"

	"github.com/majorcontext/motte/internal/vfs/store"
	"github.com/majorcontext/motte/internal/vfs/tel"
	"github.com/majorcontext/motte/internal/vfs/upperdir"
)

const defaultMaxLine = 64 << 20

// Workspace is the part of the protocol that the lower layer answers: the change token, the dirty set and the changes
// of the upper directory against the base tree.
type Workspace interface {
	BaseTree() string
	Token() (upperdir.Token, error)
	Dirty(ctx context.Context) ([]DirtyEntry, upperdir.Token, error)
	Changes(ctx context.Context) ([]ChangeEntry, upperdir.Token, error)
	Stats() Stats
}

// Server serves the control protocol of one workspace.
type Server struct {
	Store     store.Store
	Cache     *store.Cache
	Workspace Workspace
	// MaxLine is the longest request line in bytes. Zero is 64 MiB. A longer line gets a line_too_long answer, and
	// the connection stays open.
	MaxLine int

	mu    sync.Mutex
	conns map[net.Conn]struct{}
	wg    sync.WaitGroup
}

// Serve answers the protocol on l until ctx ends. It then closes every open connection and returns after the
// handlers have ended.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()
	defer func() {
		cancel()
		s.mu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.conns == nil {
			s.conns = map[net.Conn]struct{}{}
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(ctx, c)
	}
}

func (s *Server) serveConn(ctx context.Context, c net.Conn) {
	defer func() {
		_ = c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		s.wg.Done()
	}()
	rd := bufio.NewReaderSize(c, 1<<20)
	enc := json.NewEncoder(c)
	max := s.MaxLine
	if max <= 0 {
		max = defaultMaxLine
	}
	for {
		line, tooLong, err := readLine(rd, max)
		if err != nil {
			return
		}
		var ans any
		var rq request
		switch {
		case tooLong:
			ans = failure{Err: "ctl: the request line is longer than the limit", Code: CodeLineTooLong}
		case json.Unmarshal(line, &rq) != nil:
			ans = invalid("ctl: the request is not a JSON object")
		default:
			ans = s.handle(ctx, rq)
		}
		if enc.Encode(ans) != nil {
			return
		}
	}
}

// readLine reads one line without its newline. A line over max bytes is read to its end and dropped, and tooLong is
// set.
func readLine(r *bufio.Reader, max int) (line []byte, tooLong bool, err error) {
	for {
		chunk, err := r.ReadSlice('\n')
		if !tooLong {
			if len(line)+len(chunk) > max+1 {
				tooLong, line = true, nil
			} else {
				line = append(line, chunk...)
			}
		}
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case err == io.EOF && (len(line) > 0 || tooLong):
			return line, tooLong, nil
		case err != nil:
			return nil, false, err
		}
		if n := len(line); n > 0 && line[n-1] == '\n' {
			line = line[:n-1]
		}
		return line, tooLong, nil
	}
}

func (s *Server) tokenAnswer(tok upperdir.Token) tokenAnswer {
	return tokenAnswer{Boot: bootID, Token: tok.String(bootID), BaseTree: s.Workspace.BaseTree()}
}

// handle serves one request and returns the answer to encode.
func (s *Server) handle(ctx context.Context, rq request) any {
	ctx = tel.WithTraceparent(ctx, rq.Traceparent)
	switch rq.Op {
	case "token":
		tok, err := s.Workspace.Token()
		if err != nil {
			return internal(err)
		}
		return s.tokenAnswer(tok)
	case "dirty":
		es, tok, err := s.Workspace.Dirty(ctx)
		if err != nil {
			return internal(err)
		}
		return dirtyAnswer{s.tokenAnswer(tok), nonNil(es)}
	case "changes":
		cs, tok, err := s.Workspace.Changes(ctx)
		if err != nil {
			return internal(err)
		}
		return changesAnswer{s.tokenAnswer(tok), nonNil(cs)}
	case "stats":
		return s.Workspace.Stats()
	case "getbatch":
		return s.getBlobs(ctx, rq.SHAs, rq.MaxSize)
	case "get", "put", "putbatch":
		return s.objects(ctx, rq)
	case "opheads":
		if st, ok := s.Store.(store.OpHeadsStore); ok {
			return serveOpHeads(ctx, st, rq)
		}
	case "refs", "swapref":
		if st, ok := s.Store.(store.RefStore); ok {
			return serveRefs(ctx, st, rq)
		}
	case "putop", "getop":
		if st, ok := s.Store.(store.OpObjectStore); ok {
			return serveOp(ctx, st, rq)
		}
	case "has":
		if st, ok := s.Store.(store.HasStore); ok {
			return serveHas(ctx, st, rq.SHAs)
		}
	case "workspace":
		if st, ok := s.Store.(store.WorkspaceStore); ok {
			return serveWorkspace(ctx, st, rq)
		}
	}
	return failure{Err: "unknown op", Code: CodeUnknownOp}
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
