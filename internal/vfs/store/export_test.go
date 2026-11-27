package store

import "os"

func CreateLockedTempForTest(dir string) (*os.File, error) { return createLockedTemp(dir) }
