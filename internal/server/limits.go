package server

import "github.com/majorcontext/motte/internal/gitpack"

// Limits are the size limits of one server. A zero field takes its default, which is the value of the spec.
type Limits struct {
	// Object is the largest object that a put accepts, in bytes.
	Object int
	// DecodeWindow bounds the JSON text that the decoder of an object put holds for one object: 16 MiB in base64 and
	// some slack.
	DecodeWindow int64
	// TreeReserve is the first reservation of a tree read, and TreeSlack the fixed part of every later one.
	TreeReserve, TreeSlack int64
	// FetchLarge is the reservation above which a fetch takes one of the FetchSlots slots of large fetches, which a
	// clone of a large repository needs. Without the slots, a few clones hold the whole memory budget for as long as
	// they run.
	FetchLarge int64
	FetchSlots int
}

func (l Limits) withDefaults() Limits {
	if l.Object <= 0 {
		l.Object = gitpack.MaxObjectBytes
	}
	if l.DecodeWindow <= 0 {
		l.DecodeWindow = 24 << 20
	}
	if l.TreeReserve <= 0 {
		l.TreeReserve = 8 << 20
	}
	if l.TreeSlack <= 0 {
		l.TreeSlack = 8 << 20
	}
	if l.FetchLarge <= 0 {
		l.FetchLarge = 32 << 20
	}
	if l.FetchSlots <= 0 {
		l.FetchSlots = 4
	}
	return l
}
