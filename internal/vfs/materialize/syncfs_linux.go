//go:build linux

package materialize

import "golang.org/x/sys/unix"

func init() { syncFS = syncLinuxFS }

// syncLinuxFS writes back every dirty page of the file system that holds path.
func syncLinuxFS(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(fd) }()
	return unix.Syncfs(fd)
}
