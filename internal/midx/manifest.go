package midx

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Entry is one layer of a manifest. A key that ends in ".idx" names the v2 index of one pack, which is a layer of one
// pack. A key under "midx/" names a layer file of this package.
type Entry struct {
	Key     string `json:"k"`
	Objects int    `json:"n"`
	// Bytes is the total size of the packs of the layer.
	Bytes int64 `json:"b"`
	// NPacks is the number of packs of the layer.
	NPacks int `json:"np"`
	// Size and Dir are the length of a layer file and of its directory tail. They are zero for an index layer.
	Size int64 `json:"sz,omitempty"`
	Dir  int   `json:"d,omitempty"`
	// Packs lists the pack of an index layer.
	Packs []Pack `json:"p,omitempty"`
	// Class is ClassMeta or ClassBlob when every pack of the layer holds only that class of object, and empty when a
	// pack may hold both.
	Class string `json:"c,omitempty"`
}

// The classes of a layer. Metadata is commits, trees and tags.
const (
	ClassMeta = "m"
	ClassBlob = "b"
)

// IsIndex reports whether the layer is the v2 index of one pack.
func (e Entry) IsIndex() bool { return strings.HasSuffix(e.Key, ".idx") }

// Manifest is the consistent set of layers of one repository, oldest layer first.
type Manifest struct {
	Version int     `json:"v"`
	Layers  []Entry `json:"l"`
}

// Encode returns the stored form of m.
func (m Manifest) Encode() ([]byte, error) {
	m.Version = 1
	return json.Marshal(m)
}

// DecodeManifest reads the stored form.
func DecodeManifest(b []byte) (Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return Manifest{}, fmt.Errorf("midx: decode manifest: %w", err)
	}
	if m.Version != 1 {
		return Manifest{}, fmt.Errorf("midx: manifest version %d", m.Version)
	}
	return m, nil
}

// Plan is a run of the newest layers to rewrite as one.
type Plan struct{ Lo, Hi int }

// Factor is the geometric ratio. A run takes the next older layer while that layer is at most Factor times the size of
// the run, so a layer that stays is more than Factor times as large as the layer after it, and a repository of N
// objects has about log N layers.
const Factor = 2

// plan scans the end of a window from the newest layer to the oldest. A window of layers grows toward the old end
// while the next older layer is at most Factor times the window, the window stays under limit, and it has at most
// fanIn layers. The first window that is worth its cost is the plan. A run in the middle can need a merge when a
// newer layer is large, so the window does not always end at the newest layer.
func plan(layers []Entry, size func(Entry) int64, limit int64, fanIn int, worth func(lo, hi int) bool) (Plan, bool) {
	for hi := len(layers); hi >= 1; hi-- {
		lo := hi - 1
		sum := size(layers[lo])
		if sum > limit {
			continue
		}
		for lo > 0 && hi-lo < fanIn {
			prev := size(layers[lo-1])
			if prev > Factor*sum || sum+prev > limit {
				break
			}
			lo--
			sum += prev
		}
		if worth(lo, hi) {
			return Plan{lo, hi}, true
		}
	}
	return Plan{}, false
}

// PlanMerge picks the newest window of layers to merge into one layer file, by object count. The merged layer holds at
// most maxObjects records and the window at most fanIn layers.
func PlanMerge(layers []Entry, maxObjects, fanIn int) (Plan, bool) {
	return plan(layers, func(e Entry) int64 { return int64(e.Objects) }, int64(maxObjects), fanIn, func(lo, hi int) bool { return hi-lo >= 2 })
}

// PlanCompact picks the newest window of layers whose packs to rewrite as one pack, by pack bytes. The new pack has at
// most maxBytes and at most maxObjects objects, and the window holds at least two packs. A single layer that holds
// several packs, which merges of the index alone made, is a window.
func PlanCompact(layers []Entry, maxBytes int64, maxObjects, fanIn int) (Plan, bool) {
	return plan(layers, func(e Entry) int64 { return e.Bytes }, maxBytes, fanIn, func(lo, hi int) bool {
		packs, objects := 0, 0
		for _, e := range layers[lo:hi] {
			packs += e.NPacks
			objects += e.Objects
		}
		return packs >= 2 && objects <= maxObjects
	})
}
