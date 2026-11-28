package store

import "github.com/majorcontext/motte/internal/midx"

var MergeNeed = mergeNeed

func SizedLayer(objects int) midx.Entry { return midx.Entry{Objects: objects, Sizes: "midx/x.sz"} }

// OpCacheUsed is the charge that the op log cache holds.
func (s *Service) OpCacheUsed() int64 { return s.opcache.used.Load() }
