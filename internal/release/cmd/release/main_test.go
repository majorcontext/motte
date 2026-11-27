package main

import (
	"reflect"
	"strings"
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

func TestJJBuildGetsTheTagAsItsVersion(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	if err := jjCmd([]string{"-out", "x", "-arch", "amd64", "-version", "v1.2.3", "-dry-run"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "-e MOTTE_VERSION=v1.2.3 ") {
		t.Errorf("build command lacks the version:\n%s", out.String())
	}
}
