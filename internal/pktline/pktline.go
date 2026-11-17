// Package pktline reads and writes the pkt-line framing of the git wire protocols.
package pktline

import (
	"errors"
	"fmt"
	"io"
)

// MaxData is the most data bytes of one packet.
const MaxData = 65516

var ErrMalformed = errors.New("pktline: malformed packet")

type Kind int

const (
	Data Kind = iota
	Flush
	Delim
	ResponseEnd
)

// Reader reads packets from r.
type Reader struct {
	r   io.Reader
	buf []byte
}

func NewReader(r io.Reader) *Reader { return &Reader{r: r, buf: make([]byte, MaxData)} }

// Next returns the next packet. The data is valid until the next call. A clean end of input before a packet is io.EOF.
func (r *Reader) Next() ([]byte, Kind, error) {
	var head [4]byte
	if _, err := io.ReadFull(r.r, head[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, 0, ErrMalformed
		}
		return nil, 0, err
	}
	var n int
	for _, c := range head {
		d, ok := hexDigit(c)
		if !ok {
			return nil, 0, ErrMalformed
		}
		n = n<<4 | d
	}
	switch n {
	case 0:
		return nil, Flush, nil
	case 1:
		return nil, Delim, nil
	case 2:
		return nil, ResponseEnd, nil
	case 3:
		return nil, 0, ErrMalformed
	}
	n -= 4
	if n > MaxData {
		return nil, 0, ErrMalformed
	}
	if _, err := io.ReadFull(r.r, r.buf[:n]); err != nil {
		return nil, 0, ErrMalformed
	}
	return r.buf[:n], Data, nil
}

func hexDigit(c byte) (int, bool) {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0'), true
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10, true
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10, true
	}
	return 0, false
}

// Writer writes packets to w and keeps the first error.
type Writer struct {
	w   io.Writer
	err error
}

func NewWriter(w io.Writer) *Writer { return &Writer{w: w} }

// Err is the first write error.
func (w *Writer) Err() error { return w.err }

func (w *Writer) raw(b []byte) {
	if w.err == nil {
		_, w.err = w.w.Write(b)
	}
}

// Data writes one packet of at most MaxData bytes.
func (w *Writer) Data(b []byte) {
	if len(b) > MaxData {
		w.err = ErrMalformed
		return
	}
	w.raw(fmt.Appendf(make([]byte, 0, len(b)+4), "%04x%s", len(b)+4, b))
}

// Linef writes one packet of formatted text.
func (w *Writer) Linef(format string, args ...any) { w.Data(fmt.Appendf(nil, format, args...)) }

func (w *Writer) Flush() { w.raw([]byte("0000")) }

func (w *Writer) Delim() { w.raw([]byte("0001")) }

// SideBand returns a writer that sends what it gets as packets of band 1.
func SideBand(w *Writer) io.Writer { return sideBand{w} }

type sideBand struct{ w *Writer }

func (s sideBand) Write(p []byte) (int, error) {
	for off := 0; off < len(p); {
		n := min(len(p)-off, MaxData-1)
		s.w.Data(append([]byte{1}, p[off:off+n]...))
		off += n
	}
	return len(p), s.w.Err()
}
