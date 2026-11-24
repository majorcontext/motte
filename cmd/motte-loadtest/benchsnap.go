package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// A snapshot holds a generated and settled repository: the dump of the metadata database, the manifest and samples of
// gen, and a copy of the repository's bucket prefix. An arm that restores it skips gen and the settle, which are about
// half of the time of an arm. The bucket prefix of the repository is in the bucket of the host's arms, so a snapshot is
// per host: two hosts that shared a repository path would write the same objects.
type snapshot struct{ dir, bucketPrefix string }

func (o benchOpts) snapshotOf() (snapshot, bool) {
	if o.snapshot == "" {
		return snapshot{}, false
	}
	return snapshot{dir: filepath.Join(o.snapDir, o.snapshot), bucketPrefix: "gs://" + o.bucket + "/snapshots/" + o.snapshot + "/"}, true
}

func (sn snapshot) exists() bool {
	_, err := os.Stat(filepath.Join(sn.dir, "snapshot.json"))
	return err == nil
}

func repoPrefix(bucket, path string) string {
	return "gs://" + bucket + "/repos/native/" + base64.RawURLEncoding.EncodeToString([]byte(path)) + "/"
}

func gcloudCopy(ctx context.Context, from, to string) error {
	out, err := exec.CommandContext(ctx, "gcloud", "storage", "cp", "-r", "--quiet", from+"*", to).CombinedOutput()
	if err != nil {
		return fmt.Errorf("gcloud storage cp %s %s: %w: %s", from, to, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// dockerStdin runs a docker command with stdin and stdout connected to files, and the standard error in errw, or in the
// error that it returns when errw is nil.
func dockerStdin(ctx context.Context, o benchOpts, in io.Reader, out io.Writer, errw io.Writer, args ...string) error {
	words := strings.Fields(o.docker)
	c := exec.CommandContext(ctx, words[0], append(words[1:], args...)...)
	c.Stdin, c.Stdout = in, out
	var stderr strings.Builder
	c.Stderr = &stderr
	if errw != nil {
		c.Stderr = errw
	}
	if err := c.Run(); err != nil {
		return fmt.Errorf("%s %s: %w: %s", o.docker, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// save writes the snapshot of the stack that gen filled and the layers settled. The service is stopped for the dump.
func (sn snapshot) save(ctx context.Context, s stack, armDir string) error {
	var m struct {
		Manifest struct {
			Path string `json:"path"`
		} `json:"manifest"`
	}
	raw, err := os.ReadFile(filepath.Join(armDir, "manifest.json"))
	if err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(`{"manifest":`+string(raw)+`}`), &m); err != nil || m.Manifest.Path == "" {
		return errors.New("bench: the manifest of gen holds no repository path")
	}
	if err := os.MkdirAll(sn.dir, 0o755); err != nil {
		return err
	}
	if _, err := s.docker(ctx, "stop", "lt-motte"); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(sn.dir, "db.dump"))
	if err != nil {
		return err
	}
	defer f.Close()
	if err := dockerStdin(ctx, s.o, nil, f, nil, "exec", "lt-pg", "pg_dump", "-U", "postgres", "-Fc", "postgres"); err != nil {
		return err
	}
	if err := gcloudCopy(ctx, repoPrefix(s.o.bucket, m.Manifest.Path), sn.bucketPrefix); err != nil {
		return err
	}
	ents, _ := os.ReadDir(armDir)
	for _, e := range ents {
		if skip := map[string]bool{"meta.json": true, "gen.txt": true, "settle.txt": true}; e.IsDir() || skip[e.Name()] {
			continue
		}
		b, err := os.ReadFile(filepath.Join(armDir, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(sn.dir, e.Name()), b, 0o644); err != nil {
			return err
		}
	}
	meta, _ := json.Marshal(map[string]string{"repo_path": m.Manifest.Path, "bucket": s.o.bucket})
	return os.WriteFile(filepath.Join(sn.dir, "snapshot.json"), meta, 0o644)
}

// restore puts the snapshot into a stack whose PostgreSQL runs and whose service does not run yet, and copies the files of
// gen into armDir.
func (sn snapshot) restore(ctx context.Context, s stack, armDir string) error {
	raw, err := os.ReadFile(filepath.Join(sn.dir, "snapshot.json"))
	if err != nil {
		return err
	}
	var meta map[string]string
	if err := json.Unmarshal(raw, &meta); err != nil {
		return err
	}
	if meta["bucket"] != s.o.bucket {
		return fmt.Errorf("bench: the snapshot was made for bucket %s, not %s", meta["bucket"], s.o.bucket)
	}
	in, err := os.Open(filepath.Join(sn.dir, "db.dump"))
	if err != nil {
		return err
	}
	defer in.Close()
	if err := dockerStdin(ctx, s.o, in, io.Discard, nil, "exec", "-i", "lt-pg", "pg_restore", "-U", "postgres", "-d", "postgres", "--no-owner"); err != nil {
		return err
	}
	if err := gcloudCopy(ctx, sn.bucketPrefix, repoPrefix(s.o.bucket, meta["repo_path"])); err != nil {
		return err
	}
	ents, _ := os.ReadDir(sn.dir)
	for _, e := range ents {
		if e.Name() == "snapshot.json" || e.Name() == "db.dump" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(sn.dir, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(armDir, e.Name()), b, 0o644); err != nil {
			return err
		}
	}
	return nil
}
