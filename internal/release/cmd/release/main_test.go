package main

import (
	"reflect"
	"testing"
)

func TestImageTags(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		in   string
		want []string
		err  bool
	}{
		{"v1.2.3", []string{"1.2.3", "1.2"}, false},
		{"0.4.0", []string{"0.4.0", "0.4"}, false},
		{"v1.2.3-rc.1", []string{"1.2.3-rc.1"}, false},
		{"main", nil, true},
	} {
		got, err := imageTags(c.in)
		if (err != nil) != c.err || !reflect.DeepEqual(got, c.want) {
			t.Errorf("imageTags(%q) = %v, %v; want %v, err %v", c.in, got, err, c.want, c.err)
		}
	}
}
