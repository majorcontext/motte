package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// armData is what the summary takes from the output directory of one arm.
type armData struct {
	Values map[string]float64
	Layers float64
}

type pairRow struct {
	Name     string    `json:"name"`
	Better   string    `json:"better"`
	A        []float64 `json:"a"`
	B        []float64 `json:"b"`
	Ratio    []float64 `json:"ratios"`
	MedA     float64   `json:"median_a"`
	MedB     float64   `json:"median_b"`
	MedRatio float64   `json:"median_ratio"`
	Min      float64   `json:"min_ratio"`
	Max      float64   `json:"max_ratio"`
}

type pairSummary struct {
	Group string    `json:"group"`
	Pairs int       `json:"pairs"`
	Rows  []pairRow `json:"rows"`
}

func median(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := slices.Clone(v)
	sort.Float64s(s)
	if n := len(s); n%2 == 0 {
		return (s[n/2-1] + s[n/2]) / 2
	}
	return s[len(s)/2]
}

// readArm reads the run report, the server metrics and the CPU of one arm directory.
func readArm(dir string) (armData, error) {
	var rep struct {
		Ops []struct {
			Op   string  `json:"op"`
			P50  float64 `json:"p50_ms"`
			P99  float64 `json:"p99_ms"`
			Errs int     `json:"errors"`
		} `json:"ops"`
		Agents *struct {
			Tasks   float64 `json:"tasks"`
			Landed  float64 `json:"landed_per_min"`
			Reads   float64 `json:"range_reads_per_task"`
			RangeKB float64 `json:"range_kib_per_task"`
		} `json:"agents"`
		Server map[string]float64 `json:"server_counters"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		return armData{}, err
	}
	if err := json.Unmarshal(raw, &rep); err != nil {
		return armData{}, fmt.Errorf("%s: %w", dir, err)
	}
	if rep.Agents == nil {
		return armData{}, fmt.Errorf("%s: the report has no agents section", dir)
	}
	d := armData{Values: map[string]float64{"lands_per_min": rep.Agents.Landed, "reads_per_task": rep.Agents.Reads, "bytes_per_task": rep.Agents.RangeKB * 1024}}
	if b, err := os.ReadFile(filepath.Join(dir, "layers.txt")); err == nil {
		d.Layers, _ = strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	}
	for _, o := range rep.Ops {
		d.Values[o.Op+"_p50_ms"], d.Values[o.Op+"_p99_ms"] = o.P50, o.P99
	}
	for _, c := range []string{"range", "index", "pack"} {
		h, m := rep.Server["motte_"+c+"_cache_hits_total"], rep.Server["motte_"+c+"_cache_misses_total"]
		if h+m > 0 {
			d.Values[c+"_cache_hit_rate"] = h / (h + m)
		}
	}
	d.Values["layers_settled"] = d.Layers
	var cpu struct {
		Before, After map[string]float64
	}
	if raw, err := os.ReadFile(filepath.Join(dir, "cpu.json")); err == nil && json.Unmarshal(raw, &cpu) == nil && rep.Agents.Tasks > 0 {
		d.Values["cpu_s_per_task"] = (cpu.After["lt-motte"] - cpu.Before["lt-motte"]) / rep.Agents.Tasks
	}
	return d, nil
}

// summarize compares the arms of each pair: B over A for every value that both arms hold.
func summarize(group string, a, b []armData) pairSummary {
	sum := pairSummary{Group: group, Pairs: len(a)}
	names := map[string]bool{}
	for _, d := range a {
		for k := range d.Values {
			names[k] = true
		}
	}
	keys := make([]string, 0, len(names))
	for k := range names {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		row := pairRow{Name: k, Better: "lower"}
		if k == "lands_per_min" || strings.HasSuffix(k, "hit_rate") {
			row.Better = "higher"
		}
		for i := range a {
			x, okA := a[i].Values[k]
			y, okB := b[i].Values[k]
			if !okA || !okB || x == 0 {
				continue
			}
			row.A, row.B, row.Ratio = append(row.A, x), append(row.B, y), append(row.Ratio, y/x)
		}
		if len(row.Ratio) == 0 {
			continue
		}
		row.MedA, row.MedB, row.MedRatio = median(row.A), median(row.B), median(row.Ratio)
		row.Min, row.Max = slices.Min(row.Ratio), slices.Max(row.Ratio)
		sum.Rows = append(sum.Rows, row)
	}
	return sum
}

func (s pairSummary) table() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s: %d pairs, B over A\n", s.Group, s.Pairs)
	for _, r := range s.Rows {
		fmt.Fprintf(&b, "%-26s (%-6s) medA=%10.2f medB=%10.2f medB/A=%.2f [%.2f-%.2f] pairs:", r.Name, r.Better, r.MedA, r.MedB, r.MedRatio, r.Min, r.Max)
		for _, x := range r.Ratio {
			fmt.Fprintf(&b, " %.2f", x)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

// armDir is the output directory of arm side ("A" or "B") of pair i.
func armDir(out, group, side string, agents, i int) string {
	return filepath.Join(out, fmt.Sprintf("%s-%s-%d-%d", group, side, agents, i))
}

func runSummary(args []string) error {
	fs := newFlagSet("bench sum")
	out := fs.String("out", "bench", "directory of the arm directories")
	group := fs.String("group", "g", "group name of the pairs")
	agents := fs.Int("c", 64, "agents of the pairs")
	n := fs.Int("pairs", 4, "pairs to read")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var a, b []armData
	for i := 1; i <= *n; i++ {
		x, err := readArm(armDir(*out, *group, "A", *agents, i))
		if err != nil {
			return err
		}
		y, err := readArm(armDir(*out, *group, "B", *agents, i))
		if err != nil {
			return err
		}
		a, b = append(a, x), append(b, y)
	}
	s := summarize(*group, a, b)
	raw, _ := json.MarshalIndent(s, "", "  ")
	if err := os.WriteFile(filepath.Join(*out, fmt.Sprintf("%s-%d-summary.json", *group, *agents)), raw, 0o644); err != nil {
		return err
	}
	fmt.Print(s.table())
	return nil
}

// printClone reports what the cold clones of a clone run cost the service.
func printClone(dir string) error {
	var rep struct {
		Ops []struct {
			Op     string  `json:"op"`
			Count  int     `json:"count"`
			P50    float64 `json:"p50_ms"`
			P99    float64 `json:"p99_ms"`
			Errors int     `json:"errors"`
		} `json:"ops"`
		Agents *struct {
			Clones   float64 `json:"clones"`
			CloneKiB float64 `json:"clone_kib_avg"`
			Reads    float64 `json:"range_reads_per_task"`
			RangeKiB float64 `json:"range_kib_per_task"`
		} `json:"agents"`
		Server map[string]float64 `json:"server_counters"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "run.json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, &rep); err != nil || rep.Agents == nil {
		return fmt.Errorf("%s: no agents report: %v", dir, err)
	}
	for _, o := range rep.Ops {
		if o.Op == "clone" || o.Op == "fetch" {
			fmt.Printf("%-6s n=%d p50=%.0f ms p99=%.0f ms errors=%d\n", o.Op, o.Count, o.P50, o.P99, o.Errors)
		}
	}
	fmt.Printf("clones=%.0f clone size avg=%.0f KiB; per task, which holds the clone and the explore, edit, push and land steps that follow it: bucket reads=%.0f, KiB=%.0f\n", rep.Agents.Clones, rep.Agents.CloneKiB, rep.Agents.Reads, rep.Agents.RangeKiB)
	for _, k := range []string{"motte_bucket_range_reads_total", "motte_bucket_range_read_bytes_total", "motte_bucket_index_read_bytes_total", "motte_range_cache_hits_total", "motte_range_cache_misses_total"} {
		fmt.Printf("%s %.0f\n", k, rep.Server[k])
	}
	return nil
}
