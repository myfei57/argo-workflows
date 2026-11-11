//go:build !windows

package provenance

import (
	"io/fs"
	"syscall"
)

// fileIdentity returns the (device, inode) pair underlying fi so
// hardlinked regular files are hashed once and linked by reference.
// ok is false when the platform file info does not expose the pair.
func fileIdentity(fi fs.FileInfo) (dev, ino uint64, ok bool) {
	stat, isStat := fi.Sys().(*syscall.Stat_t)
	if !isStat {
		return 0, 0, false
	}
	return uint64(stat.Dev), uint64(stat.Ino), true
}
