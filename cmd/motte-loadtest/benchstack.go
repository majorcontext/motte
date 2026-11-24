package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedStrings(s []string) []string { sort.Strings(s); return s }

type stack struct {
	o   benchOpts
	bin string
	key string
}

func (s stack) docker(ctx context.Context, args ...string) ([]byte, error) {
	return dockerCmd(ctx, s.o, args...)
}

func (s stack) down(ctx context.Context) {
	_, _ = s.docker(ctx, "rm", "-fv", "lt-motte", "lt-pg", "lt-client")
	_, _ = s.docker(ctx, "network", "rm", "ltnet")
}

func (s stack) startServer(ctx context.Context, compaction bool) error {
	args := []string{"run", "-d", "--name", "lt-motte", "--network", "ltnet", "--cpuset-cpus", s.o.srvCPUs,
		"-e", "MOTTE_BUCKET_KIND=gcs", "-e", "MOTTE_BUCKET=" + s.o.bucket, "-v", "/etc/ssl/certs:/etc/ssl/certs:ro",
		"-e", "MOTTE_DATABASE_URL=postgres://postgres:postgres@lt-pg:5432/postgres?sslmode=disable",
		"-e", "MOTTE_COMPACTION=" + strconv.FormatBool(compaction), "-e", "MOTTE_LISTEN=:8080", "-e", "MOTTE_TRUSTED_KEYS=" + s.key}
	if adc, _ := filepath.Abs(os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")); os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		args = append(args, "-v", adc+":/adc.json:ro", "-e", "GOOGLE_APPLICATION_CREDENTIALS=/adc.json")
	}
	for _, e := range s.o.env {
		args = append(args, "-e", e)
	}
	bin, err := filepath.Abs(s.bin)
	if err != nil {
		return err
	}
	args = append(args, "-v", bin+":/motte:ro", "alpine", "/motte")
	if _, err := s.docker(ctx, args...); err != nil {
		return err
	}
	return s.waitFor(ctx, func() bool {
		_, err := s.docker(ctx, "exec", "lt-motte", "wget", "-qO-", "http://127.0.0.1:8080/ready")
		return err == nil
	}, 10*time.Minute, "the service to load the layers")
}

func (s stack) waitFor(ctx context.Context, ok func() bool, max time.Duration, what string) error {
	for end := time.Now().Add(max); time.Now().Before(end); time.Sleep(time.Second) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if ok() {
			return nil
		}
	}
	return fmt.Errorf("bench: %s did not come up within %v", what, max)
}

var metricLine = regexp.MustCompile(`^(motte_\S+) (\S+)$`)

func (s stack) metrics(ctx context.Context) ([]byte, error) {
	return s.docker(ctx, "exec", "lt-motte", "wget", "-qO-", "http://127.0.0.1:8080/metrics")
}

func (s stack) metricValue(ctx context.Context, name string) (float64, error) {
	b, err := s.metrics(ctx)
	if err != nil {
		return 0, err
	}
	for _, l := range strings.Split(string(b), "\n") {
		if m := metricLine.FindStringSubmatch(l); m != nil && m[1] == name {
			return strconv.ParseFloat(m[2], 64)
		}
	}
	return 0, fmt.Errorf("metric %s is missing", name)
}

func (s stack) cpu(ctx context.Context) (map[string]float64, error) {
	out := map[string]float64{}
	for _, c := range []string{"lt-motte", "lt-pg"} {
		b, err := s.docker(ctx, "exec", c, "cat", "/sys/fs/cgroup/cpu.stat")
		if err != nil {
			return nil, fmt.Errorf("bench: the CPU of %s needs cgroup v2: %w", c, err)
		}
		for _, l := range strings.Split(string(b), "\n") {
			if f := strings.Fields(l); len(f) == 2 && f[0] == "usage_usec" {
				v, _ := strconv.ParseFloat(f[1], 64)
				out[c] = v / 1e6
			}
		}
		if _, ok := out[c]; !ok {
			return nil, fmt.Errorf("bench: cpu.stat of %s has no usage_usec", c)
		}
	}
	return out, nil
}

// client runs the load client and returns its standard output. Its standard error goes to dir/<cmd>.err.
func (s stack) client(ctx context.Context, cmd string, args []string, dir string) ([]byte, error) {
	key, _ := filepath.Abs(s.o.keyFile)
	lt, _ := filepath.Abs(s.o.loadtest)
	dir, _ = filepath.Abs(dir)
	a := []string{"run", "--rm", "--name", "lt-client", "--network", "ltnet", "--cpuset-cpus", s.o.cliCPUs,
		"-v", lt + ":/lt:ro", "-v", key + ":/motte.key:ro", "-v", dir + ":/ltdir", "alpine", "/lt"}
	var out, errb bytes.Buffer
	err := dockerStdin(ctx, s.o, nil, &out, &errb, append(a, append([]string{cmd}, args...)...)...)
	_ = armFile(dir, cmd+".err", errb.Bytes())
	return out.Bytes(), err
}

// settled waits until the layers of every repository stay at or under the limit for three samples in a row, and returns the
// last count. The metric is 0 until a maintenance pass has seen a repository, so 0 does not count as settled.
func (s stack) settled(ctx context.Context, dir string) (int, error) {
	var log strings.Builder
	defer func() { _ = armFile(dir, "settle.txt", []byte(log.String())) }()
	ok := 0
	for start := time.Now(); time.Since(start) < s.o.settleMax; {
		v, err := s.metricValue(ctx, "motte_manifest_layers_max")
		fmt.Fprintf(&log, "+%ds layers=%v err=%v\n", int(time.Since(start).Seconds()), v, err)
		if err == nil && v > 0 && int(v) <= s.o.settle && time.Since(start) >= s.o.settleMin {
			if ok++; ok >= 3 {
				_ = armFile(dir, "layers.txt", []byte(strconv.Itoa(int(v))+"\n"))
				return int(v), nil
			}
		} else {
			ok = 0
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
	return 0, fmt.Errorf("bench: the layers did not settle to %d within %v", s.o.settle, s.o.settleMax)
}

// runArm runs one arm: it imports the repository, waits for the layers to settle, optionally turns maintenance off, runs the
// load, checks the flags that the client applied, and writes the directory of the arm.
func runArm(ctx context.Context, o benchOpts, bin, dir string) (err error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	key, err := publicKey(o.keyFile)
	if err != nil {
		return err
	}
	s := stack{o: o, bin: bin, key: key}
	s.down(ctx)
	defer s.down(context.WithoutCancel(ctx))
	args, want := o.runArgs()
	meta, _ := json.Marshal(map[string]any{"binary": bin, "started": time.Now().UTC(), "run_args": args, "options": o.summary()})
	if err := armFile(dir, "meta.json", meta); err != nil {
		return err
	}
	for _, c := range [][]string{
		{"network", "create", "ltnet"},
		{"run", "-d", "--name", "lt-pg", "--network", "ltnet", "--cpuset-cpus", o.srvCPUs, "-e", "POSTGRES_PASSWORD=postgres", o.pgImage},
	} {
		if _, err := s.docker(ctx, c...); err != nil {
			return err
		}
	}
	if err := s.waitFor(ctx, func() bool {
		_, err := s.docker(ctx, "exec", "lt-pg", "pg_isready", "-h", "127.0.0.1", "-U", "postgres")
		return err == nil
	}, 90*time.Second, "PostgreSQL"); err != nil {
		return err
	}
	snap, useSnap := o.snapshotOf()
	restore := useSnap && snap.exists()
	if restore {
		if err := snap.restore(ctx, s, dir); err != nil {
			return err
		}
	}
	if restore {
		if err := s.startServer(ctx, o.compaction); err != nil {
			return err
		}
	} else {
		if err := s.startServer(ctx, true); err != nil {
			return err
		}
		gen, err := s.client(ctx, "gen", []string{"-url", "http://lt-motte:8080", "-key", "/motte.key", "-out", "/ltdir", "-objects", strconv.Itoa(o.objects), "-files", strconv.Itoa(o.files)}, dir)
		_ = armFile(dir, "gen.txt", gen)
		if err != nil {
			return err
		}
		if _, err := s.settled(ctx, dir); err != nil {
			return err
		}
		if useSnap {
			if err := snap.save(ctx, s, dir); err != nil {
				return err
			}
		}
		if useSnap || !o.compaction {
			_, _ = s.docker(ctx, "rm", "-f", "lt-motte")
			if err := s.startServer(ctx, o.compaction); err != nil {
				return err
			}
		}
	}
	before, _ := s.metrics(ctx)
	cpuBefore, err := s.cpu(ctx)
	if err != nil {
		return err
	}
	rep, err := s.client(ctx, "run", args[1:], dir)
	if err != nil {
		return err
	}
	after, _ := s.metrics(ctx)
	cpuAfter, err := s.cpu(ctx)
	if err != nil {
		return err
	}
	cpu, _ := json.Marshal(map[string]any{"Before": cpuBefore, "After": cpuAfter})
	for name, data := range map[string][]byte{"run.json": rep, "metrics.before": before, "metrics.after": after, "cpu.json": cpu, "argv.txt": []byte(strings.Join(args, " ") + "\n")} {
		if err := armFile(dir, name, data); err != nil {
			return err
		}
	}
	var applied struct {
		Applied  map[string]string `json:"applied_flags"`
		Manifest struct {
			Path string `json:"path"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(rep, &applied); err != nil {
		return fmt.Errorf("bench: the run report is not JSON: %w", err)
	}
	if o.cleanup && applied.Manifest.Path != "" {
		prefix := "gs://" + o.bucket + "/repos/native/" + base64.RawURLEncoding.EncodeToString([]byte(applied.Manifest.Path)) + "/"
		out, _ := exec.CommandContext(ctx, "gcloud", "storage", "rm", "-r", "--quiet", prefix).CombinedOutput()
		_ = armFile(dir, "cleanup.txt", append([]byte(prefix+"\n"), out...))
	}
	return checkApplied(want, applied.Applied)
}

func (o benchOpts) summary() map[string]any {
	return map[string]any{"bucket": o.bucket, "agents": o.agents, "duration": o.duration.String(), "scenario": o.scenario, "cold_rate": o.coldRate,
		"objects": o.objects, "files": o.files, "settle": o.settle, "compaction": o.compaction, "server_cpus": o.srvCPUs, "client_cpus": o.cliCPUs, "env": []string(o.env)}
}
