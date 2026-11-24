package store

import "github.com/majorcontext/motte/internal/midx"

var MergeNeed = mergeNeed

func SizedLayer(objects int) midx.Entry { return midx.Entry{Objects: objects, Sizes: "midx/x.sz"} }
