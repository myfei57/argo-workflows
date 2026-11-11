//go:build windows

package provenance

import "io/fs"

// Windows file info does not expose a stable (device, inode) pair through
// os.FileInfo.Sys, so hardlink identity is not observable here. Every
// regular file is treated as a primary entry and hashed independently;
// the hardlink protocol stays explicit on platforms that expose it.
func fileIdentity(_ fs.FileInfo) (dev, ino uint64, ok bool) {
	return 0, 0, false
}
