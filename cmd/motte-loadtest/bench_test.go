package main

import (
	"strings"
	"testing"
	"time"
)

func TestSummaryGivesPerPairRatiosAndMedians(t *testing.T) {
	t.Parallel()
	arm := func(lands, p50 float64) armData {
		return armData{Values: map[string]float64{"lands_per_min": lands, "land_p50_ms": p50}}
	}
	s := summarize("g", []armData{arm(100, 1000), arm(200, 2000), arm(100, 3000)}, []armData{arm(110, 1500), arm(200, 2000), arm(50, 3300)})
	rows := map[string]pairRow{}
	for _, r := range s.Rows {
		rows[r.Name] = r
	}
	if r := rows["lands_per_min"]; r.MedRatio != 1 || r.Min != 0.5 || r.Max != 1.1 || r.Better != "higher" {
		t.Errorf("lands/min row = %+v, want median 1, range 0.5 to 1.1, higher is better", r)
	}
	if r := rows["land_p50_ms"]; r.MedA != 2000 || r.MedRatio != 1.1 || r.Better != "lower" {
		t.Errorf("land p50 row = %+v, want median A 2000 and median ratio 1.1", r)
	}
	if got := s.table(); !strings.Contains(got, "medB/A=1.10") || !strings.Contains(got, "3 pairs") {
		t.Errorf("table lacks the ratios:\n%s", got)
	}
}

func TestARunFailsWhenTheClientAppliedOtherFlags(t *testing.T) {
	t.Parallel()
	o := benchOpts{agents: 64, duration: 3 * time.Minute, scenario: "vfsd", runFlags: "-skew=0.5"}
	_, want := o.runArgs()
	applied := map[string]string{"c": "64", "d": "3m0s", "scenario": "vfsd", "cold-rate": "0", "skew": "0.5", "url": "u", "key": "k", "out": "o", "json": "true"}
	if err := checkApplied(want, applied); err != nil {
		t.Fatalf("flags that came back as asked failed: %v", err)
	}
	applied["d"] = "180s"
	if err := checkApplied(want, applied); err != nil {
		t.Fatalf("the same duration in another spelling failed: %v", err)
	}
	applied["d"] = "3m0s"
	for name, change := range map[string]func(){"changed": func() { applied["c"] = "16" }, "dropped": func() { delete(applied, "skew") }, "extra": func() { applied["think"] = "1s" }} {
		change()
		if err := checkApplied(want, applied); err == nil {
			t.Errorf("a %s flag passed the check", name)
		}
		applied["c"], applied["skew"] = "64", "0.5"
		delete(applied, "think")
	}
}
