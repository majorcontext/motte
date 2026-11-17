//go:build linux

// vfsfuzz applies identical random edit sequences to a kernel overlay over a motte-vfsd lower layer and to a
// plain git worktree (the oracle), then applies the dirty set of the daemon to the base tree and compares the
// result with `git add -A && git write-tree`.
package main

import (
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/majorcontext/motte/internal/vfs/testdata/overlaykit"
	"golang.org/x/sys/unix"
)

var (
	mnt       = flag.String("mnt", "/workspace", "daemon mount")
	shadow    = flag.String("shadow", "/tmp/shadow", "oracle worktree")
	gitdir    = flag.String("gitdir", "/tmp/shadow.git", "oracle git dir")
	sock      = flag.String("sock", ctlSock(), "control socket of the mount")
	seqs      = flag.Int("seqs", 1000, "number of sequences")
	seed      = flag.Int64("seed", 1, "rng seed")
	subs      = flag.String("subtrees", "src/cmd/go,doc,test,misc", "directories edits are confined to")
	fullEvery = flag.Int("full", 250, "full-walk compare every N sequences")
	skipSync  = flag.Bool("nosync", false, "do not syncfs before reading the dirty set (expect failures under exclusive)")
	maxOps    = flag.Int("maxops", 8, "max ops per sequence")
	verbose   = flag.Bool("v", false, "log ops")
	freshIdx  = flag.Bool("freshindex", true, "drop oracle index before each add")
	failFast  = flag.Bool("failfast", true, "stop at first mismatch")
	linkWrite = flag.Bool("linkwrite", true, "the link op also writes through the new name")
)

var rng *rand.Rand
var hist = map[string]int{}
var counter int

func must(err error) {
	if err != nil {
		panic(err)
	}
}

// ctlSock is MOTTE_VFS_SOCK, else the path where a workspace sees the socket.
func ctlSock() string {
	if p := os.Getenv("MOTTE_VFS_SOCK"); p != "" {
		return p
	}
	return "/run/motte-vfs/ctl.sock"
}

func git(args ...string) string {
	cmd := exec.Command("git", args...)
	cmd.Env = append(os.Environ(), "GIT_DIR="+*gitdir, "GIT_WORK_TREE="+*shadow)
	out, err := cmd.CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("git %v: %v\n%s", args, err, out))
	}
	return strings.TrimSpace(string(out))
}

// randDir returns a random existing directory under a confined subtree.
func randDir() string {
	roots := strings.Split(*subs, ",")
	d := roots[rng.Intn(len(roots))]
	for i := 0; i < 4 && rng.Intn(3) > 0; i++ {
		ents, err := os.ReadDir(filepath.Join(*shadow, d))
		if err != nil {
			break
		}
		var ds []string
		for _, e := range ents {
			if e.IsDir() {
				ds = append(ds, e.Name())
			}
		}
		if len(ds) == 0 {
			break
		}
		d = filepath.Join(d, ds[rng.Intn(len(ds))])
	}
	return d
}

func randFile() (string, bool) {
	d := randDir()
	ents, err := os.ReadDir(filepath.Join(*shadow, d))
	if err != nil {
		return "", false
	}
	var fs []string
	for _, e := range ents {
		if e.Type().IsRegular() {
			fs = append(fs, e.Name())
		}
	}
	if len(fs) == 0 {
		return "", false
	}
	return filepath.Join(d, fs[rng.Intn(len(fs))]), true
}

func randAny() (string, bool) {
	d := randDir()
	ents, err := os.ReadDir(filepath.Join(*shadow, d))
	if err != nil || len(ents) == 0 {
		return "", false
	}
	return filepath.Join(d, ents[rng.Intn(len(ents))].Name()), true
}

func newName() string {
	counter++
	return fmt.Sprintf("fz_%d_%d", counter, rng.Intn(1000))
}

func content() []byte {
	var n int
	switch rng.Intn(10) {
	case 0:
		n = 0
	case 1:
		n = 200_000 + rng.Intn(300_000)
	default:
		n = rng.Intn(6000)
	}
	b := make([]byte, n)
	rng.Read(b)
	return b
}

func errno(err error) string {
	if err == nil {
		return ""
	}
	var e syscall.Errno
	if errors.As(err, &e) {
		return e.Error()
	}
	return err.Error()
}

type op struct {
	name string
	run  func(root string) error
}

func smallDir(rel string) bool {
	n := 0
	filepath.WalkDir(filepath.Join(*shadow, rel), func(p string, d fs.DirEntry, err error) error {
		n++
		if n > 300 {
			return fs.SkipAll
		}
		return nil
	})
	return n <= 300
}

func pickOp() *op {
	if rng.Intn(4) == 0 {
		return pickOverlayOp()
	}
	switch k := rng.Intn(18); k {
	case 0, 1:
		d := randDir()
		name := filepath.Join(d, newName()+".txt")
		data := content()
		return &op{"newfile " + name, func(r string) error { return os.WriteFile(filepath.Join(r, name), data, 0o644) }}
	case 2:
		f, ok := randFile()
		if !ok {
			return nil
		}
		data := content()
		return &op{"overwrite " + f, func(r string) error { return os.WriteFile(filepath.Join(r, f), data, 0o644) }}
	case 3:
		f, ok := randFile()
		if !ok {
			return nil
		}
		data := content()
		return &op{"append " + f, func(r string) error {
			h, err := os.OpenFile(filepath.Join(r, f), os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return err
			}
			defer h.Close()
			_, err = h.Write(data)
			return err
		}}
	case 4:
		f, ok := randFile()
		if !ok {
			return nil
		}
		sz := int64(rng.Intn(9000))
		return &op{fmt.Sprintf("truncate %s %d", f, sz), func(r string) error { return os.Truncate(filepath.Join(r, f), sz) }}
	case 5:
		f, ok := randFile()
		if !ok {
			return nil
		}
		off := int64(rng.Intn(5000))
		data := make([]byte, rng.Intn(100)+1)
		rng.Read(data)
		return &op{fmt.Sprintf("pwrite %s@%d", f, off), func(r string) error {
			h, err := os.OpenFile(filepath.Join(r, f), os.O_RDWR, 0)
			if err != nil {
				return err
			}
			defer h.Close()
			_, err = h.WriteAt(data, off)
			return err
		}}
	case 6:
		f, ok := randFile()
		if !ok {
			return nil
		}
		off := rng.Intn(3000)
		data := content()
		if len(data) > 2000 {
			data = data[:2000]
		}
		if len(data) == 0 {
			data = []byte("x")
		}
		msync := rng.Intn(2) == 0
		return &op{fmt.Sprintf("mmapwrite %s@%d msync=%v", f, off, msync), func(r string) error {
			h, err := os.OpenFile(filepath.Join(r, f), os.O_RDWR, 0)
			if err != nil {
				return err
			}
			defer h.Close()
			st, err := h.Stat()
			if err != nil {
				return err
			}
			size := int(st.Size())
			if off+len(data) > size {
				size = off + len(data)
				if err := h.Truncate(int64(size)); err != nil {
					return err
				}
			}
			m, err := unix.Mmap(int(h.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
			if err != nil {
				return err
			}
			copy(m[off:], data)
			if msync {
				if err := unix.Msync(m, unix.MS_SYNC); err != nil {
					return err
				}
			}
			return unix.Munmap(m)
		}}
	case 7:
		f, ok := randFile()
		if !ok {
			return nil
		}
		return &op{"rm " + f, func(r string) error { return os.Remove(filepath.Join(r, f)) }}
	case 8:
		d := randDir()
		if strings.Count(d, "/") < 2 || !smallDir(d) {
			return nil
		}
		return &op{"rm -rf " + d, func(r string) error { return os.RemoveAll(filepath.Join(r, d)) }}
	case 9:
		d := randDir()
		name := filepath.Join(d, newName())
		data := content()
		return &op{"mkdir+file " + name, func(r string) error {
			if err := os.MkdirAll(filepath.Join(r, name, "sub"), 0o755); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(r, name, "sub", "a.txt"), data, 0o644)
		}}
	case 10, 11:
		f, ok := randFile()
		if !ok {
			return nil
		}
		var to string
		if rng.Intn(3) == 0 {
			if g, ok := randFile(); ok {
				to = g
			}
		}
		if to == "" {
			to = filepath.Join(randDir(), newName()+".go")
		}
		if to == f {
			return nil
		}
		return &op{"rename " + f + " -> " + to, func(r string) error { return os.Rename(filepath.Join(r, f), filepath.Join(r, to)) }}
	case 12:
		d := randDir()
		if strings.Count(d, "/") < 2 || !smallDir(d) {
			return nil
		}
		to := filepath.Join(randDir(), newName())
		if strings.HasPrefix(to, d+"/") || to == d {
			return nil
		}
		return &op{"renamedir " + d + " -> " + to, func(r string) error { return os.Rename(filepath.Join(r, d), filepath.Join(r, to)) }}
	case 13:
		name := filepath.Join(randDir(), newName()+".lnk")
		target := fmt.Sprintf("../t%d/x", rng.Intn(100))
		return &op{"symlink " + name, func(r string) error { return os.Symlink(target, filepath.Join(r, name)) }}
	case 14:
		f, ok := randFile()
		if !ok {
			return nil
		}
		mode := os.FileMode(0o755)
		if rng.Intn(2) == 0 {
			mode = 0o644
		}
		return &op{fmt.Sprintf("chmod %s %o", f, mode), func(r string) error { return os.Chmod(filepath.Join(r, f), mode) }}
	case 15:
		f, ok := randFile()
		if !ok {
			return nil
		}
		data := content()
		return &op{"atomicreplace " + f, func(r string) error {
			tmp := filepath.Join(r, f+".tmp-fz")
			if err := os.WriteFile(tmp, data, 0o644); err != nil {
				return err
			}
			return os.Rename(tmp, filepath.Join(r, f))
		}}
	case 16:
		d := randDir()
		if strings.Count(d, "/") < 2 || !smallDir(d) {
			return nil
		}
		data := content()
		return &op{"recreatedir " + d, func(r string) error {
			if err := os.RemoveAll(filepath.Join(r, d)); err != nil {
				return err
			}
			if err := os.Mkdir(filepath.Join(r, d), 0o755); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(r, d, "again.txt"), data, 0o644)
		}}
	default:
		f, ok := randAny()
		if !ok {
			return nil
		}
		return &op{"readcheck " + f, func(r string) error {
			st, err := os.Lstat(filepath.Join(r, f))
			if err != nil {
				return err
			}
			if st.Mode().IsRegular() {
				b, err := os.ReadFile(filepath.Join(r, f))
				if err != nil {
					return err
				}
				return fmt.Errorf("sum:%x", sha256.Sum256(b))
			}
			return nil
		}}
	}
}

func syncfs() {
	if *skipSync {
		return
	}
	fd, err := unix.Open(*mnt, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	must(err)
	defer unix.Close(fd)
	must(unix.Syncfs(fd))
}

type ent struct {
	mode string
	sum  string
}

func walk(root string) map[string]ent {
	out := map[string]ent{}
	filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		must(err)
		rel, _ := filepath.Rel(root, p)
		if rel == ".git" || rel == ".jj" {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if rel == "." {
			return nil
		}
		st, err := os.Lstat(p)
		must(err)
		switch {
		case st.IsDir():
			out[rel] = ent{mode: "d"}
		case st.Mode()&os.ModeSymlink != 0:
			t, _ := os.Readlink(p)
			out[rel] = ent{mode: "l", sum: t}
		case st.Mode()&(os.ModeNamedPipe|os.ModeSocket|os.ModeDevice) != 0:
			out[rel] = ent{mode: "s"}
		default:
			h := sha256.New()
			f, err := os.Open(p)
			must(err)
			io.Copy(h, f)
			f.Close()
			m := "f"
			if st.Mode()&0o100 != 0 {
				m = "x"
			}
			out[rel] = ent{mode: m, sum: fmt.Sprintf("%x:%d", h.Sum(nil)[:8], st.Size())}
		}
		return nil
	})
	return out
}

func fullCompare() string {
	a, b := walk(*mnt), walk(*shadow)
	var diffs []string
	for p, e := range a {
		if o, ok := b[p]; !ok {
			diffs = append(diffs, "only in mount: "+p)
		} else if o != e {
			diffs = append(diffs, fmt.Sprintf("differs: %s mount=%v shadow=%v", p, e, o))
		}
	}
	for p := range b {
		if _, ok := a[p]; !ok {
			_, err := os.Lstat(filepath.Join(*mnt, p))
			ents, _ := os.ReadDir(filepath.Dir(filepath.Join(*mnt, p)))
			listed := false
			for _, e := range ents {
				if e.Name() == filepath.Base(p) {
					listed = true
				}
			}
			diffs = append(diffs, fmt.Sprintf("only in shadow: %s (lstat on mount now: %v; listed now: %v)", p, err, listed))
		}
	}
	sort.Strings(diffs)
	if len(diffs) > 10 {
		diffs = diffs[:10]
	}
	return strings.Join(diffs, "\n")
}

func main() {
	flag.Parse()
	rng = rand.New(rand.NewSource(*seed))
	syscall.Umask(0o022)

	requireSocket()
	r, err := overlaykit.Ctl{Sock: *sock}.Call("dirty")
	must(err)
	base := r.BaseTree
	requireCleanUpper(r)
	// oracle: worktree checked out from the same tree
	os.RemoveAll(*shadow)
	os.RemoveAll(*gitdir)
	must(os.MkdirAll(*shadow, 0o755))
	if out, err := exec.Command("git", "init", "-q", "--bare", *gitdir).CombinedOutput(); err != nil {
		panic(string(out))
	}
	altSrc := os.Getenv("ALT_OBJECTS")
	must(os.WriteFile(filepath.Join(*gitdir, "objects/info/alternates"), []byte(strings.ReplaceAll(altSrc, ":", "\n")+"\n"), 0o644))
	if err := (overlaykit.Git{Dir: *gitdir}).RefuseGitlinks(base); err != nil {
		fmt.Fprintln(os.Stderr, "vfsfuzz refuses:", err)
		os.Exit(2)
	}
	git("read-tree", base)
	git("checkout-index", "-a", "-f")
	git("config", "core.fileMode", "true")
	fmt.Println("oracle ready at", base)

	bad := 0
	t0 := time.Now()
	for s := 0; s < *seqs; s++ {
		n := 1 + rng.Intn(*maxOps)
		var log []string
		for i := 0; i < n; i++ {
			o := pickOp()
			if o == nil {
				continue
			}
			kind := strings.Fields(o.name)[0]
			hist[kind]++
			e1 := o.run(*mnt)
			e2 := o.run(*shadow)
			log = append(log, o.name)
			if *verbose {
				fmt.Println(o.name, "|", errno(e1))
			}
			if kind == "readcheck" {
				if (e1 == nil) != (e2 == nil) || (e1 != nil && e1.Error() != e2.Error()) {
					fmt.Printf("SEQ %d READ MISMATCH %s: mount=%v shadow=%v\n", s, o.name, e1, e2)
					bad++
				}
				continue
			}
			if kind == "setxattr" {
				if e1 != nil {
					looseFail++
				}
			} else if errno(e1) != errno(e2) {
				fmt.Printf("SEQ %d ERRNO MISMATCH %s: mount=%q shadow=%q\n", s, o.name, errno(e1), errno(e2))
				bad++
			}
		}
		syncfs()
		bad += overlayCheck(s, log)
		if (s+1)%*fullEvery == 0 {
			if d := fullCompare(); d != "" {
				bad++
				fmt.Printf("SEQ %d FULL WALK MISMATCH\n%s\n", s, d)
			} else {
				fmt.Printf("seq %d ok (dirty set and full walk equal) elapsed=%s\n", s+1, time.Since(t0).Round(time.Second))
			}
		}
		if bad > 0 && *failFast {
			break
		}
	}
	fmt.Printf("RESULT seqs=%d bad=%d elapsed=%s dirty_avg=%.1f token_retries=%d xattr_op_errors=%d ops=%v\n",
		*seqs, bad, time.Since(t0).Round(time.Second), float64(overlayDirty)/float64(*seqs), tokenRetries, looseFail, hist)
	if bad > 0 {
		os.Exit(1)
	}
}
