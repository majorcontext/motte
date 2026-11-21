package main

import (
	"bufio"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func scrape(base string) map[string]float64 {
	out := map[string]float64{}
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		return out
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		l := sc.Text()
		if !strings.HasPrefix(l, "motte_pack_cache") && !strings.HasPrefix(l, "motte_bucket") && !strings.HasPrefix(l, "motte_index_cache") {
			continue
		}
		k, v, ok := strings.Cut(l, " ")
		if f, err := strconv.ParseFloat(v, 64); ok && err == nil {
			out[k] = f
		}
	}
	return out
}

type watchReport struct {
	AvgCores   float64 `json:"avg_cores"`
	PeakCores  float64 `json:"peak_cores"`
	PeakRSSMiB int64   `json:"peak_rss_mib"`
	Procs      int     `json:"procs_peak"`
}

type watcher struct {
	stopc chan struct{}
	wg    sync.WaitGroup
	mu    sync.Mutex
	out   map[string]watchReport
}

func startWatch(spec string) *watcher {
	w := &watcher{stopc: make(chan struct{}), out: map[string]watchReport{}}
	for _, part := range strings.Split(spec, ",") {
		name, sub, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		w.wg.Add(1)
		go w.sample(name, sub)
	}
	return w
}

func (w *watcher) stop() map[string]watchReport {
	close(w.stopc)
	w.wg.Wait()
	return w.out
}

func procTotals(sub string) (ticks uint64, rssKiB int64, n int) {
	ents, _ := os.ReadDir("/proc")
	for _, de := range ents {
		if _, err := strconv.Atoi(de.Name()); err != nil {
			continue
		}
		cmd, err := os.ReadFile("/proc/" + de.Name() + "/cmdline")
		if err != nil || !strings.Contains(string(cmd), sub) {
			continue
		}
		stat, err := os.ReadFile("/proc/" + de.Name() + "/stat")
		if err != nil {
			continue
		}
		s := string(stat)
		rp := strings.LastIndexByte(s, ')')
		f := strings.Fields(s[rp+2:])
		if len(f) < 13 {
			continue
		}
		u, _ := strconv.ParseUint(f[11], 10, 64)
		k, _ := strconv.ParseUint(f[12], 10, 64)
		ticks += u + k
		st, _ := os.ReadFile("/proc/" + de.Name() + "/status")
		for _, line := range strings.Split(string(st), "\n") {
			if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
				kb, _ := strconv.ParseInt(strings.Fields(v)[0], 10, 64)
				rssKiB += kb
			}
		}
		n++
	}
	return
}

func (w *watcher) sample(name, sub string) {
	defer w.wg.Done()
	const hz = 100.0
	var rep watchReport
	t0 := time.Now()
	startTicks, _, _ := procTotals(sub)
	last, lastT := startTicks, t0
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for {
		select {
		case <-w.stopc:
			ticks, _, _ := procTotals(sub)
			if d := time.Since(t0).Seconds(); d > 0 && ticks >= startTicks {
				rep.AvgCores = float64(ticks-startTicks) / hz / d
			}
			w.mu.Lock()
			w.out[name] = rep
			w.mu.Unlock()
			return
		case now := <-tk.C:
			ticks, rss, n := procTotals(sub)
			if dt := now.Sub(lastT).Seconds(); dt > 0 && ticks >= last {
				rep.PeakCores = max(rep.PeakCores, float64(ticks-last)/hz/dt)
			}
			last, lastT = ticks, now
			rep.PeakRSSMiB = max(rep.PeakRSSMiB, rss/1024)
			rep.Procs = max(rep.Procs, n)
		}
	}
}

var _ = io.Discard
