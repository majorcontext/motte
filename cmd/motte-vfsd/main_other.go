//go:build !linux

package main

import "os"

func main() {
	_, _ = os.Stderr.WriteString("motte-vfsd runs on Linux only\n")
	os.Exit(1)
}
