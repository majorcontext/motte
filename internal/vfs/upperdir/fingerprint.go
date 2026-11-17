package upperdir

import (
	"fmt"
	"sync/atomic"
	"time"
)

// SettleWindow is how long the newest change time must lie before the start of a walk for the walk to be
// settled. Kernel timestamps are coarse, so a change that lands in the same tick as the walk can leave the
// fingerprint unchanged.
const SettleWindow = 50 * time.Millisecond

var unsettledSeq atomic.Uint64

// Token is the fingerprint of an upper directory. The sums of the change times and of the inode numbers
// catch what the maximum alone misses: a clock that steps back, and a file replaced by another of the same
// size. An unsettled token carries a number that no other token has.
type Token struct {
	Entries  int64
	Ctime    int64
	Bytes    int64
	SumCtime uint64
	SumIno   uint64
	Settled  bool
	nonce    uint64
}

// Same reports whether two tokens show the same upper directory. An unsettled token is never the same.
func (t Token) Same(o Token) bool { return t.Settled && o.Settled && t == o }

// String formats the token for the wire, behind the boot ID of the daemon process.
func (t Token) String(boot string) string {
	if !t.Settled {
		return fmt.Sprintf("%s:unsettled.%d", boot, t.nonce)
	}
	return fmt.Sprintf("%s:%d.%d.%d.%x.%x", boot, t.Entries, t.Ctime, t.Bytes, t.SumCtime, t.SumIno)
}

func unsettledToken() Token { return Token{nonce: unsettledSeq.Add(1)} }

// Fingerprint walks the upper directory with lstat only. now is the start of the walk. The root entries
// that Scan skips do not count. The token does not see a second store through a mapping that is already
// dirty: the change time moves at the first write fault only.
func Fingerprint(fsys FS, root string, now time.Time) (Token, error) {
	d, err := fsys.Open(root)
	if err != nil {
		return Token{}, err
	}
	defer func() { _ = d.Close() }()
	st, err := d.Stat()
	if err != nil {
		return Token{}, err
	}
	t := Token{Ctime: st.Ctime}
	if err := walkPrint(d, "", &t); err != nil {
		return Token{}, err
	}
	t.Settled = now.UnixNano()-t.Ctime >= int64(SettleWindow)
	if !t.Settled {
		t = unsettledToken()
	}
	return t, nil
}

func walkPrint(d Dir, m string, t *Token) error {
	ents, err := d.Entries()
	if err != nil {
		if notExist(err) {
			return nil
		}
		return err
	}
	for _, e := range ents {
		if reserved(m, e.Name) {
			continue
		}
		t.Entries++
		t.Ctime = max(t.Ctime, e.Ctime)
		t.SumCtime += uint64(e.Ctime)
		t.SumIno += e.Ino
		switch {
		case e.Mode.IsDir():
			cd, err := d.Open(e.Name)
			if notExist(err) {
				continue
			}
			if err != nil {
				return err
			}
			err = walkPrint(cd, join(m, e.Name), t)
			_ = cd.Close()
			if err != nil {
				return err
			}
		case e.Mode.IsRegular():
			t.Bytes += e.Size
		}
	}
	return nil
}
