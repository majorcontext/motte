package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const benchUsage = `usage: motte-loadtest bench arm|pairs|sum|clone [flags]
  arm    run one stack of this host (a service on a real bucket, PostgreSQL and the load client in containers) and write its directory
  pairs  run -n pairs of arms of two service binaries, in alternating order, and summarize them
  sum    summarize the pairs that are in a directory
  clone  run one agent whose every task starts from a cold partial clone, and report the reads that a clone costs`

func newFlagSet(name string) *flag.FlagSet { return flag.NewFlagSet(name, flag.ContinueOnError) }

func runBench(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New(benchUsage)
	}
	switch args[0] {
	case "arm", "pairs", "clone":
		return runBenchRuns(ctx, args[0], args[1:])
	case "sum":
		return runSummary(args[1:])
	}
	return errors.New(benchUsage)
}

// benchOpts is what one arm needs. The service binary of an arm is given apart.
type benchOpts struct {
	docker, loadtest, keyFile, bucket, out, group       string
	lock, srvCPUs, cliCPUs, scenario, pgImage, runFlags string
	snapshot, snapDir                                   string
	agents, objects, files, settle, pairs               int
	duration, settleMin, settleMax                      time.Duration
	coldRate                                            float64
	compaction, cleanup                                 bool
	env                                                 stringList
}

type stringList []string

func (l *stringList) String() string     { return strings.Join(*l, ",") }
func (l *stringList) Set(v string) error { *l = append(*l, v); return nil }

func (o *benchOpts) register(fs *flag.FlagSet) {
	exe, _ := os.Executable()
	fs.StringVar(&o.docker, "docker", "docker", "the docker command, such as \"sudo docker\"")
	fs.StringVar(&o.loadtest, "loadtest", exe, "linux binary of motte-loadtest that the client container runs")
	fs.StringVar(&o.keyFile, "key", "motte.key", "Ed25519 PKCS#8 key file; its public key is the trusted key of the service")
	fs.StringVar(&o.bucket, "bucket", "", "the Google Cloud Storage bucket of the service; it uses the credentials of the host")
	fs.StringVar(&o.out, "out", "bench", "output directory")
	fs.StringVar(&o.group, "group", "g", "group name of the arms")
	fs.StringVar(&o.lock, "lock", "/tmp/motte-bench.lock", "lock file: one stack runs on a host at a time")
	fs.StringVar(&o.srvCPUs, "server-cpus", "0-13", "cpuset of the service and PostgreSQL")
	fs.StringVar(&o.cliCPUs, "client-cpus", "14-15", "cpuset of the load client")
	fs.StringVar(&o.scenario, "scenario", "vfsd", "scenario of the load client")
	fs.StringVar(&o.pgImage, "postgres", "postgres:16", "PostgreSQL image")
	fs.StringVar(&o.runFlags, "run-flags", "", "more flags of the load client's run command, as -name=value words; they are checked like the others")
	fs.StringVar(&o.snapshot, "snapshot", "", "name of a snapshot of the generated repository: the first arm makes it, later arms restore it and skip gen and the settle; make the name differ for each host")
	fs.StringVar(&o.snapDir, "snapshot-dir", "bench-snapshots", "directory of the snapshots")
	fs.IntVar(&o.agents, "c", 64, "agents")
	fs.IntVar(&o.objects, "objects", 400000, "objects of the repository")
	fs.IntVar(&o.files, "files", 20000, "files of the repository")
	fs.IntVar(&o.settle, "settle", 8, "wait until no repository has more than this many layers")
	fs.IntVar(&o.pairs, "n", 4, "pairs")
	fs.DurationVar(&o.duration, "d", 3*time.Minute, "run time")
	fs.DurationVar(&o.settleMin, "settle-min", 40*time.Second, "samples of the settle count only after this time, so that a maintenance pass (every 30 seconds by default) has seen the generated repository")
	fs.DurationVar(&o.settleMax, "settle-max", 15*time.Minute, "longest wait for the layers to settle")
	fs.Float64Var(&o.coldRate, "cold-rate", 0, "chance that a task starts from a cold partial clone")
	fs.BoolVar(&o.compaction, "compaction", false, "run maintenance during the run; it is always on while the layers settle")
	fs.BoolVar(&o.cleanup, "cleanup", true, "delete the repository from the bucket after the arm (needs gcloud)")
	fs.Var(&o.env, "env", "NAME=value for the service; repeatable")
}

// runArgs returns the arguments that the load client gets, and the flags that must come back as applied.
func (o benchOpts) runArgs() (args []string, want map[string]string) {
	want = map[string]string{
		"c": strconv.Itoa(o.agents), "d": o.duration.String(), "scenario": o.scenario,
		"cold-rate": strconv.FormatFloat(o.coldRate, 'g', -1, 64),
	}
	for _, w := range strings.Fields(o.runFlags) {
		k, v, _ := strings.Cut(strings.TrimLeft(w, "-"), "=")
		want[k] = v
	}
	args = []string{"run", "-url", "http://lt-motte:8080", "-key", "/motte.key", "-out", "/ltdir", "-json"}
	for _, k := range sortedKeys(want) {
		args = append(args, "-"+k+"="+want[k])
	}
	return args, want
}

var fixedRunFlags = map[string]bool{"url": true, "key": true, "out": true, "json": true}

// checkApplied fails when the load client applied other flags than the run asked for.
func checkApplied(want, applied map[string]string) error {
	var bad []string
	for k, v := range want {
		if got, ok := applied[k]; !ok || !sameValue(got, v) {
			bad = append(bad, fmt.Sprintf("-%s asked %q, applied %q", k, v, applied[k]))
		}
	}
	for k := range applied {
		if _, ok := want[k]; !ok && !fixedRunFlags[k] {
			bad = append(bad, fmt.Sprintf("-%s applied without being asked", k))
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("the load client applied other flags than the run asked for: %s", strings.Join(sortedStrings(bad), "; "))
	}
	return nil
}

// sameValue reports whether two spellings of a flag value mean one value: the flag package prints 1s for 1000ms and 0s for 0.
func sameValue(a, b string) bool {
	if a == b {
		return true
	}
	if x, err := time.ParseDuration(a); err == nil {
		y, err := time.ParseDuration(b)
		return err == nil && x == y
	}
	if x, err := strconv.ParseFloat(a, 64); err == nil {
		y, err := strconv.ParseFloat(b, 64)
		return err == nil && x == y
	}
	return false
}

func runBenchRuns(ctx context.Context, cmd string, args []string) error {
	var o benchOpts
	fs := newFlagSet("bench " + cmd)
	o.register(fs)
	if cmd == "clone" {
		o.duration = 10 * time.Minute
	}
	binA := fs.String("a", "", "service binary of arm A (pairs); the only binary of arm and clone")
	binB := fs.String("b", "", "service binary of arm B (pairs)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if o.bucket == "" || *binA == "" || (cmd == "pairs" && *binB == "") {
		return errors.New("bench: -bucket and -a are required, and -b with pairs")
	}
	if cmd == "clone" {
		o.agents, o.scenario, o.coldRate = 1, "agents", 1
		o.runFlags += " -first-cold=true"
	}
	unlock, err := lockHost(o.lock)
	if err != nil {
		return err
	}
	defer unlock()
	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return err
	}
	if cmd != "pairs" {
		dir := armDir(o.out, o.group, "A", o.agents, 1)
		if err := runArm(ctx, o, *binA, dir); err != nil {
			return err
		}
		if cmd == "clone" {
			return printClone(dir)
		}
		return nil
	}
	for i := 1; i <= o.pairs; i++ {
		sides := [][2]string{{"A", *binA}, {"B", *binB}}
		if i%2 == 0 {
			sides[0], sides[1] = sides[1], sides[0]
		}
		for _, s := range sides {
			if err := runArm(ctx, o, s[1], armDir(o.out, o.group, s[0], o.agents, i)); err != nil {
				return err
			}
		}
	}
	return runSummary([]string{"-out", o.out, "-group", o.group, "-c", strconv.Itoa(o.agents), "-pairs", strconv.Itoa(o.pairs)})
}

func publicKey(keyFile string) (string, error) {
	s, err := loadSigner(keyFile, "local")
	if err != nil {
		return "", err
	}
	pub := s.Key.Public().(ed25519.PublicKey)
	keys := []map[string]any{{"kid": "local", "key": base64.StdEncoding.EncodeToString(pub), "scopes": []string{"node", "read", "write", "import", "operator", "merge"}}}
	b, _ := json.Marshal(keys)
	return string(b), nil
}

func dockerCmd(ctx context.Context, o benchOpts, args ...string) ([]byte, error) {
	words := strings.Fields(o.docker)
	c := exec.CommandContext(ctx, words[0], append(words[1:], args...)...)
	out, err := c.CombinedOutput()
	if err != nil {
		return out, fmt.Errorf("%s %s: %w: %s", o.docker, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return out, nil
}

func armFile(dir, name string, data []byte) error {
	return os.WriteFile(filepath.Join(dir, name), data, 0o644)
}
