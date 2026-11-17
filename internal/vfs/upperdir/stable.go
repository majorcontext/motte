package upperdir

import (
	"context"
	"time"
)

const stableTries = 3

// ScanStable reads the token, scans, and reads the token again, and retries when the two differ. An
// unsettled first token ends it after one scan, because the consumer treats that token as a change. It
// returns the token read before the winning scan, so an edit made during the scan shows in the next token.
// After the last try it returns the last scan with an unsettled token, which no other token equals.
func ScanStable(ctx context.Context, fsys FS, root string, base BaseTree, now func() time.Time) ([]Entry, Token, error) {
	var es []Entry
	for range stableTries {
		before, err := Fingerprint(fsys, root, now())
		if err != nil {
			return nil, Token{}, err
		}
		if es, err = Scan(ctx, fsys, root, base); err != nil {
			return nil, Token{}, err
		}
		if !before.Settled {
			return es, before, nil
		}
		after, err := Fingerprint(fsys, root, now())
		if err != nil {
			return nil, Token{}, err
		}
		if before.Same(after) {
			return es, before, nil
		}
	}
	return es, unsettledToken(), nil
}
