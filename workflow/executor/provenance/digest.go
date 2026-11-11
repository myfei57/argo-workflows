package provenance

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// ErrBudgetExceeded means a tree build crossed an explicit byte, file
// or temp-disk budget. No success receipt is written for that artifact.
var ErrBudgetExceeded = errors.New("provenance budget exceeded")

const sha256Name = "sha256"

// StreamHasher accumulates a SHA-256 and a byte count in one write pass.
// Use it with io.TeeReader so a stream is digested while it is consumed
// once: it must never be wrapped around a retry of a reader that has
// already been consumed.
type StreamHasher struct {
	h     hash.Hash
	bytes int64
}

// NewStreamHasher returns an empty single-pass hasher.
func NewStreamHasher() *StreamHasher {
	return &StreamHasher{h: sha256.New()}
}

func (s *StreamHasher) Write(p []byte) (int, error) {
	n, err := s.h.Write(p)
	s.bytes += int64(n)
	return n, err
}

// Bytes returns the number of bytes hashed so far.
func (s *StreamHasher) Bytes() int64 { return s.bytes }

// Digest returns the complete digest; call it only after the stream
// reached EOF without error.
func (s *StreamHasher) Digest() Digest {
	return Digest{Algorithm: sha256Name, Value: hex.EncodeToString(s.h.Sum(nil)), Bytes: s.bytes, Status: StatusComplete}
}

// DigestReader consumes r exactly once and returns its digest. A read
// error, including a partial stream, returns an error and no digest:
// the caller must not turn the partial hash into a receipt.
func DigestReader(r io.Reader, budget Budget) (Digest, error) {
	h := NewStreamHasher()
	if err := copyBounded(h, r, budget.MaxBytes, 0); err != nil {
		return Digest{}, err
	}
	return h.Digest(), nil
}

// DigestFile hashes the exact bytes of a local file in one streaming
// read. It never re-reads to retry and opens the file read-only.
func DigestFile(filePath string, budget Budget) (Digest, error) {
	f, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return Digest{}, fmt.Errorf("provenance: open %s for digest: %w", filePath, err)
	}
	defer func() { _ = f.Close() }()
	d, err := DigestReader(bufio.NewReader(f), budget)
	if err != nil {
		return Digest{}, fmt.Errorf("provenance: digest %s: %w", filePath, err)
	}
	return d, nil
}

// copyBounded copies src into dst while enforcing the per-tree byte
// budget. alreadyHashed lets archive summarizers carry the running
// total across entries.
func copyBounded(dst io.Writer, src io.Reader, maxBytes, alreadyHashed int64) error {
	if maxBytes <= 0 {
		return fmt.Errorf("%w: byte budget is not configured", ErrBudgetExceeded)
	}
	// 1 MiB window keeps memory flat regardless of artifact size.
	buf := make([]byte, 1024*1024)
	var total int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			if alreadyHashed+total+int64(n) > maxBytes {
				return fmt.Errorf("%w: more than %d content bytes", ErrBudgetExceeded, maxBytes)
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
			total += int64(n)
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// treeBuilder carries the shared state of one normalized tree walk:
// canonical root hash, counters, hardlink index and budgets.
type treeBuilder struct {
	budget      Budget
	root        string
	rootHash    hash.Hash
	entries     []TreeEntry
	emptyDirs   []string
	seenParents map[string]struct{}
	// hardlinks maps (dev,ino) to the primary relative path.
	hardlinks map[fileIdentityKey]string
	// contentByPath lets tar hardlink entries reuse a prior digest.
	contentByPath map[string]Digest
	files         int
	dirs          int
	symlinks      int
	hardlinksSeen int
	totalBytes    int64
}

type fileIdentityKey struct {
	dev uint64
	ino uint64
}

func newTreeBuilder(root string, budget Budget) *treeBuilder {
	return &treeBuilder{
		budget:        budget,
		root:          filepath.Clean(root),
		rootHash:      sha256.New(),
		entries:       []TreeEntry{},
		seenParents:   map[string]struct{}{},
		hardlinks:     map[fileIdentityKey]string{},
		contentByPath: map[string]Digest{},
	}
}

// BuildTree walks root without following symlinks and produces the
// normalized tree summary. The walk is lexical, so identical trees
// produce identical root digests independently of filesystem order.
func BuildTree(root string, budget Budget) (*TreeSummary, error) {
	if budget.MaxFiles <= 0 || budget.MaxBytes <= 0 {
		return nil, fmt.Errorf("%w: provenance tree budget must set MaxFiles and MaxBytes", ErrBudgetExceeded)
	}
	tb := newTreeBuilder(root, budget)
	walkErr := filepath.WalkDir(root, func(full string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if len(tb.entries) >= budget.MaxFiles {
			return fmt.Errorf("%w: more than %d tree entries", ErrBudgetExceeded, budget.MaxFiles)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(tb.root, full)
		if err != nil {
			return err
		}
		relPath := filepath.ToSlash(rel)
		if relPath == "." {
			relPath = "."
		}
		return tb.addEntry(relPath, full, info)
	})
	if walkErr != nil {
		return nil, walkErr
	}
	tb.collectEmptyDirs()
	return tb.summary(), nil
}

func (tb *treeBuilder) addEntry(relPath, full string, info fs.FileInfo) error {
	entry := TreeEntry{Path: relPath, Mode: uint32(info.Mode().Perm())}
	mode := info.Mode()
	switch {
	case mode&os.ModeSymlink != 0:
		entry.Type = TreeEntrySymlink
		target, err := os.Readlink(full)
		if err != nil {
			return fmt.Errorf("provenance: read symlink %s: %w", relPath, err)
		}
		entry.LinkTarget = filepath.ToSlash(target)
		within := symlinkWithinRoot(tb.root, full, target)
		entry.WithinRoot = &within
		tb.symlinks++
	case mode.IsDir():
		entry.Type = TreeEntryDir
		tb.dirs++
	case mode.IsRegular():
		entry.Type = TreeEntryFile
		entry.Size = info.Size()
		if dev, ino, ok := fileIdentity(info); ok {
			key := fileIdentityKey{dev: dev, ino: ino}
			if primary, seen := tb.hardlinks[key]; seen {
				// Hardlink protocol: the secondary entry references the
				// primary's content digest instead of re-reading bytes.
				entry.Type = TreeEntryHardlink
				entry.HardlinkOf = primary
				primaryDigest := tb.contentByPath[primary]
				entry.Content = &primaryDigest
				tb.hardlinksSeen++
				tb.entries = append(tb.entries, entry)
				tb.seenParents[path.Dir(relPath)] = struct{}{}
				return tb.canonical(entry)
			}
			tb.hardlinks[key] = relPath
		}
		if tb.totalBytes+info.Size() > tb.budget.MaxBytes {
			return fmt.Errorf("%w: tree %s larger than %d bytes", ErrBudgetExceeded, tb.root, tb.budget.MaxBytes)
		}
		digest, err := DigestFile(full, tb.budget)
		if err != nil {
			return err
		}
		entry.Content = &digest
		tb.contentByPath[relPath] = digest
		tb.totalBytes += info.Size()
		tb.files++
	default:
		// Devices, sockets, named pipes and other non-regular nodes are
		// recorded by path and mode without content reads.
		entry.Type = "other"
	}
	tb.entries = append(tb.entries, entry)
	tb.seenParents[path.Dir(relPath)] = struct{}{}
	return tb.canonical(entry)
}

// canonical feeds one stable line per entry into the root digest. The
// line carries every identity-relevant field so two trees differ if
// any path, type, mode, size, content, link or hardlink differs.
func (tb *treeBuilder) canonical(e TreeEntry) error {
	contentHex := ""
	if e.Content != nil {
		contentHex = e.Content.Value
	}
	within := ""
	if e.WithinRoot != nil {
		within = strconv.FormatBool(*e.WithinRoot)
	}
	line := strings.Join([]string{
		e.Path,
		e.Type,
		fmt.Sprintf("%04o", e.Mode),
		strconv.FormatInt(e.Size, 10),
		contentHex,
		e.LinkTarget,
		within,
		e.HardlinkOf,
	}, "|")
	_, err := tb.rootHash.Write([]byte(line + "\n"))
	return err
}

func (tb *treeBuilder) collectEmptyDirs() {
	hasChild := map[string]bool{}
	for _, e := range tb.entries {
		if e.Path == "." {
			continue
		}
		hasChild[path.Dir(e.Path)] = true
	}
	for _, e := range tb.entries {
		if e.Type == TreeEntryDir && !hasChild[e.Path] {
			tb.emptyDirs = append(tb.emptyDirs, e.Path)
		}
	}
}

func (tb *treeBuilder) summary() *TreeSummary {
	return &TreeSummary{
		RootDigest: Digest{
			Algorithm: sha256Name,
			Value:     hex.EncodeToString(tb.rootHash.Sum(nil)),
			Bytes:     tb.totalBytes,
			Status:    StatusComplete,
		},
		Entries:       tb.entries,
		FileCount:     tb.files,
		DirCount:      tb.dirs,
		SymlinkCount:  tb.symlinks,
		HardlinkCount: tb.hardlinksSeen,
		TotalBytes:    tb.totalBytes,
		EmptyDirs:     tb.emptyDirs,
		Budget:        tb.budget,
		Status:        StatusComplete,
	}
}

// symlinkWithinRoot decides whether target stays inside root without
// following it. Absolute targets are resolved against root literally;
// relative targets against the directory holding the link.
func symlinkWithinRoot(root, linkPath, target string) bool {
	rootClean := filepath.Clean(root)
	var candidate string
	if filepath.IsAbs(target) {
		candidate = filepath.Clean(target)
	} else {
		candidate = filepath.Clean(filepath.Join(filepath.Dir(linkPath), target))
	}
	if candidate == rootClean {
		return true
	}
	return strings.HasPrefix(candidate, rootClean+string(os.PathSeparator))
}

// archiveEntry adds a tree entry synthesized from an archive header
// rather than a filesystem walk.
func (tb *treeBuilder) archiveEntry(relPath string, mode os.FileMode, size int64, entryType, linkTarget, hardlinkOf string, content *Digest, within *bool) error {
	if len(tb.entries) >= tb.budget.MaxFiles {
		return fmt.Errorf("%w: more than %d archive entries", ErrBudgetExceeded, tb.budget.MaxFiles)
	}
	entry := TreeEntry{
		Path:       relPath,
		Type:       entryType,
		Mode:       uint32(mode.Perm()),
		Size:       size,
		LinkTarget: linkTarget,
		HardlinkOf: hardlinkOf,
		Content:    content,
		WithinRoot: within,
	}
	switch entryType {
	case TreeEntryFile:
		tb.files++
		if content != nil {
			tb.totalBytes += content.Bytes
			tb.contentByPath[relPath] = *content
		}
	case TreeEntryDir:
		tb.dirs++
	case TreeEntrySymlink:
		tb.symlinks++
	case TreeEntryHardlink:
		tb.hardlinksSeen++
	}
	tb.entries = append(tb.entries, entry)
	tb.seenParents[path.Dir(relPath)] = struct{}{}
	return tb.canonical(entry)
}

// cleanArchiveRel normalizes an archive member name to a slash-separated
// root-relative path. It rejects absolute names and parent traversal.
func cleanArchiveRel(name string) (string, error) {
	slashed := filepath.ToSlash(filepath.Clean(name))
	slashed = strings.TrimPrefix(slashed, "./")
	if path.IsAbs(slashed) || slashed == ".." || strings.HasPrefix(slashed, "../") {
		return "", fmt.Errorf("provenance: archive member %q escapes the artifact root", name)
	}
	if slashed == "." || slashed == "" {
		return ".", nil
	}
	return slashed, nil
}

// SummarizeTarGz reads a gzip-compressed tar stream once, hashing the
// compressed transport bytes and the unpacked tree in the same pass.
// It must be the only consumer of r: a partially consumed stream cannot
// be retried.
func SummarizeTarGz(r io.Reader, budget Budget) (transport Digest, tree *TreeSummary, err error) {
	return summarizeTar(r, true, budget)
}

// SummarizeTar reads an uncompressed tar stream once.
func SummarizeTar(r io.Reader, budget Budget) (Digest, *TreeSummary, error) {
	return summarizeTar(r, false, budget)
}

func summarizeTar(r io.Reader, gzipped bool, budget Budget) (Digest, *TreeSummary, error) {
	transportHasher := NewStreamHasher()
	teed := io.TeeReader(r, transportHasher)
	var tarReader *tar.Reader
	if gzipped {
		gzr, err := gzip.NewReader(teed)
		if err != nil {
			return Digest{}, nil, fmt.Errorf("provenance: open gzip stream: %w", err)
		}
		defer func() { _ = gzr.Close() }()
		tarReader = tar.NewReader(gzr)
	} else {
		tarReader = tar.NewReader(teed)
	}
	tb := newTreeBuilder("", budget)
	for {
		hdr, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return Digest{}, nil, fmt.Errorf("provenance: read tar entry: %w", err)
		}
		rel, err := cleanArchiveRel(hdr.Name)
		if err != nil {
			return Digest{}, nil, err
		}
		mode := os.FileMode(hdr.Mode)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := tb.archiveEntry(rel, mode, 0, TreeEntryDir, "", "", nil, nil); err != nil {
				return Digest{}, nil, err
			}
		case tar.TypeSymlink:
			within := archiveLinkWithinRoot(rel, hdr.Linkname)
			if err := tb.archiveEntry(rel, mode, 0, TreeEntrySymlink, filepath.ToSlash(hdr.Linkname), "", nil, &within); err != nil {
				return Digest{}, nil, err
			}
		case tar.TypeLink:
			target, err := cleanArchiveRel(hdr.Linkname)
			if err != nil {
				return Digest{}, nil, err
			}
			primary, ok := tb.contentByPath[target]
			if !ok {
				return Digest{}, nil, fmt.Errorf("provenance: hardlink %q references unknown member %q", rel, target)
			}
			dup := primary
			if err := tb.archiveEntry(rel, mode, 0, TreeEntryHardlink, "", target, &dup, nil); err != nil {
				return Digest{}, nil, err
			}
		case tar.TypeReg, tar.TypeRegA:
			if tb.totalBytes+hdr.Size > budget.MaxBytes {
				return Digest{}, nil, fmt.Errorf("%w: tar content larger than %d bytes", ErrBudgetExceeded, budget.MaxBytes)
			}
			d, err := DigestReader(tarReader, Budget{MaxBytes: budget.MaxBytes - tb.totalBytes, MaxFiles: budget.MaxFiles})
			if err != nil {
				return Digest{}, nil, err
			}
			if err := tb.archiveEntry(rel, mode, hdr.Size, TreeEntryFile, "", "", &d, nil); err != nil {
				return Digest{}, nil, err
			}
		default:
			// Long names, pax headers and other special members carry no
			// file content; skip them rather than invent a type.
			continue
		}
	}
	tb.collectEmptyDirs()
	return transportHasher.Digest(), tb.summary(), nil
}

// archiveLinkWithinRoot applies the same no-escape rule as the
// filesystem walk to a link stored inside an archive.
func archiveLinkWithinRoot(rel, target string) bool {
	if path.IsAbs(filepath.ToSlash(target)) {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(path.Join(path.Dir(rel), target)))
	return clean != ".." && !strings.HasPrefix(clean, "../")
}

// SummarizeZip hashes the transport file and then parses the zip index.
// Zip requires a seekable reader, so the transport hash and the parse
// are two local reads of the same file; no network or driver call is
// repeated.
func SummarizeZip(filePath string, budget Budget) (Digest, *TreeSummary, error) {
	transport, err := DigestFile(filePath, budget)
	if err != nil {
		return Digest{}, nil, err
	}
	zr, err := zip.OpenReader(filepath.Clean(filePath))
	if err != nil {
		return Digest{}, nil, fmt.Errorf("provenance: open zip %s: %w", filePath, err)
	}
	defer func() { _ = zr.Close() }()
	tb := newTreeBuilder("", budget)
	for _, f := range zr.File {
		rel, err := cleanArchiveRel(f.Name)
		if err != nil {
			return Digest{}, nil, err
		}
		mode := f.Mode()
		switch {
		case mode&os.ModeSymlink != 0:
			rc, err := f.Open()
			if err != nil {
				return Digest{}, nil, err
			}
			raw, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				return Digest{}, nil, err
			}
			target := string(raw)
			within := archiveLinkWithinRoot(rel, target)
			if err := tb.archiveEntry(rel, mode, 0, TreeEntrySymlink, filepath.ToSlash(target), "", nil, &within); err != nil {
				return Digest{}, nil, err
			}
		case f.FileInfo().IsDir():
			if err := tb.archiveEntry(rel, mode, 0, TreeEntryDir, "", "", nil, nil); err != nil {
				return Digest{}, nil, err
			}
		default:
			if tb.totalBytes+int64(f.UncompressedSize64) > budget.MaxBytes {
				return Digest{}, nil, fmt.Errorf("%w: zip content larger than %d bytes", ErrBudgetExceeded, budget.MaxBytes)
			}
			rc, err := f.Open()
			if err != nil {
				return Digest{}, nil, err
			}
			d, err := DigestReader(rc, Budget{MaxBytes: budget.MaxBytes - tb.totalBytes, MaxFiles: budget.MaxFiles})
			_ = rc.Close()
			if err != nil {
				return Digest{}, nil, err
			}
			if err := tb.archiveEntry(rel, mode, int64(f.UncompressedSize64), TreeEntryFile, "", "", &d, nil); err != nil {
				return Digest{}, nil, err
			}
		}
	}
	tb.collectEmptyDirs()
	return transport, tb.summary(), nil
}

// Archive format hints understood by SummarizePath.
const (
	FormatTarGzip = "tar+gzip"
	FormatTar     = "tar"
	FormatZip     = "zip"
	FormatNone    = "none"
)

// SummarizePath builds the transport digest and normalized tree for a
// staged local artifact. Directories are walked in place; tar archives
// are summarized in a single streaming pass; zip archives need one
// extra local read for their seekable index; plain files produce a
// one-entry tree. Nothing here calls a storage driver or writes a temp
// copy of the artifact.
func SummarizePath(localPath, format string, budget Budget) (Digest, *TreeSummary, error) {
	fi, err := os.Lstat(localPath)
	if err != nil {
		return Digest{}, nil, fmt.Errorf("provenance: stat %s: %w", localPath, err)
	}
	if fi.IsDir() {
		tree, err := BuildTree(localPath, budget)
		if err != nil {
			return Digest{}, nil, err
		}
		// A directory has no single transport byte stream; its digest
		// is the tree root digest and the representation says so.
		return tree.RootDigest, tree, nil
	}
	switch format {
	case FormatTarGzip:
		f, err := os.Open(filepath.Clean(localPath))
		if err != nil {
			return Digest{}, nil, err
		}
		defer func() { _ = f.Close() }()
		return SummarizeTarGz(bufio.NewReader(f), budget)
	case FormatTar:
		f, err := os.Open(filepath.Clean(localPath))
		if err != nil {
			return Digest{}, nil, err
		}
		defer func() { _ = f.Close() }()
		return SummarizeTar(bufio.NewReader(f), budget)
	case FormatZip:
		return SummarizeZip(localPath, budget)
	default:
		transport, err := DigestFile(localPath, budget)
		if err != nil {
			return Digest{}, nil, err
		}
		tree, err := singleFileTree(localPath, transport, budget)
		if err != nil {
			return Digest{}, nil, err
		}
		return transport, tree, nil
	}
}

func singleFileTree(localPath string, content Digest, budget Budget) (*TreeSummary, error) {
	fi, err := os.Stat(filepath.Clean(localPath))
	if err != nil {
		return nil, fmt.Errorf("provenance: stat %s: %w", localPath, err)
	}
	name := filepath.ToSlash(filepath.Base(localPath))
	within := true
	entry := TreeEntry{Path: name, Type: TreeEntryFile, Mode: uint32(fi.Mode().Perm()), Size: content.Bytes, Content: &content, WithinRoot: &within}
	tb := newTreeBuilder("", budget)
	if err := tb.canonical(entry); err != nil {
		return nil, err
	}
	tb.entries = []TreeEntry{entry}
	tb.files = 1
	tb.totalBytes = content.Bytes
	return tb.summary(), nil
}

// multipartETagPattern matches the composite ETag S3-compatible stores
// return for multipart uploads: 32 lowercase hex digits, a dash and the
// part count. A single-part ETag has no dash and is still an MD5, not a
// SHA, so callers record both kinds under ETag rather than content.
var multipartETagPattern = regexp.MustCompile(`^[a-f0-9]{32}-[0-9]+$`)

// IsMultipartETag reports whether etag has the multipart composite
// shape. Quoted ETags are unquoted first. The result never promotes an
// ETag to a content digest.
func IsMultipartETag(etag string) bool {
	trimmed := strings.Trim(strings.TrimSpace(etag), `"`)
	return multipartETagPattern.MatchString(trimmed)
}

// UnknownDigest returns the explicit Unknown marker for facts the
// executor could not observe, for example an external input whose
// backend exposes no version or digest.
func UnknownDigest() Digest {
	return Digest{Algorithm: Unknown, Value: Unknown, Bytes: -1, Status: StatusUnknown}
}
