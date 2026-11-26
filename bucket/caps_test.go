package bucket

import "testing"

type base struct {
	Store
	caps Caps
}

func (b base) Caps() Caps { return b.caps }

type plain struct{ Store }

type layer struct{ Store }

func (l layer) Unwrap() Store { return l.Store }

func TestCapsOfFollowsUnwrapAndTheOutermostDeclarationWins(t *testing.T) {
	t.Parallel()
	del := base{caps: Caps{ConditionalDelete: true}}
	cases := []struct {
		name string
		s    Store
		want Caps
	}{
		{"no declaration", plain{}, Caps{}},
		{"declared", del, Caps{ConditionalDelete: true}},
		{"through wrappers", layer{layer{del}}, Caps{ConditionalDelete: true}},
		{"a wrapper that does not unwrap hides the store", plain{del}, Caps{}},
		{"hedged keeps the delete of the primary", Hedge(layer{del}, del, HedgeConfig{}), Caps{ConditionalDelete: true, Hedged: true}},
		{"hedged over a store without it", layer{Hedge(base{}, base{}, HedgeConfig{})}, Caps{Hedged: true}},
		{"an outer declaration overrides", layer{outer{layer{del}}}, Caps{}},
	}
	for _, c := range cases {
		if got := CapsOf(c.s); got != c.want {
			t.Errorf("%s: CapsOf = %+v, want %+v", c.name, got, c.want)
		}
	}
}

type outer struct{ Store }

func (outer) Caps() Caps      { return Caps{} }
func (o outer) Unwrap() Store { return o.Store }
