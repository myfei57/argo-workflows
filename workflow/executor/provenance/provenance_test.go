package provenance

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBudget() Budget {
	return Budget{MaxBytes: 64 * 1024 * 1024, MaxFiles: 10000, MaxTempDiskBytes: 64 * 1024 * 1024}
}

func sha256Hex(t *testing.T, data []byte) string {
	t.Helper()
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestDigestFile_KnownValue(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "f.txt")
	content := []byte("provenance digest content\n")
	require.NoError(t, os.WriteFile(p, content, 0o644))

	d, err := DigestFile(p, testBudget())
	require.NoError(t, err)
	assert.Equal(t, "sha256", d.Algorithm)
	assert.Equal(t, sha256Hex(t, content), d.Value)
	assert.Equal(t, int64(len(content)), d.Bytes)
	assert.Equal(t, StatusComplete, d.Status)
}

// erroringReader fails after delivering part of its content, simulating a
// broken transfer. A partial read must surface an error, never a digest.
type erroringReader struct {
	remaining int
	err       error
}

func (r *erroringReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, r.err
	}
	n := min(len(p), r.remaining)
	for i := 0; i < n; i++ {
		p[i] = 'a'
	}
	r.remaining -= n
	if r.remaining == 0 {
		return n, r.err
	}
	return n, nil
}

func TestDigestReader_PartialStreamFailureNoReceipt(t *testing.T) {
	streamErr := errors.New("connection reset by peer")
	r := &erroringReader{remaining: 128, err: streamErr}
	d, err := DigestReader(r, testBudget())
	require.ErrorIs(t, err, streamErr)
	assert.Equal(t, Digest{}, d, "a partial stream must not produce a digest")

	// The same behavior through StreamHasher: callers that tee a stream
	// must check the stream error before accepting Digest().
	h := NewStreamHasher()
	_, err = io.Copy(h, &erroringReader{remaining: 64, err: streamErr})
	require.ErrorIs(t, err, streamErr)
}

func TestDigestReader_BudgetEnforced(t *testing.T) {
	_, err := DigestReader(bytes.NewReader(make([]byte, 1024)), Budget{MaxBytes: 128, MaxFiles: 10})
	require.ErrorIs(t, err, ErrBudgetExceeded)
}

func TestBuildTree_FileDirSymlinkEmptyDir(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "sub", "emptydir"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "a.txt"), []byte("alpha"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(root, "sub", "b.txt"), []byte("beta-beta"), 0o600))
	// Relative symlink staying inside the root: recorded, not followed.
	require.NoError(t, os.Symlink(filepath.Join("sub", "b.txt"), filepath.Join(root, "link.txt")))
	// Escaping symlink: flagged withinRoot=false and never traversed.
	require.NoError(t, os.Symlink(filepath.Join("..", "..", "outside.txt"), filepath.Join(root, "escaping.txt")))

	tree, err := BuildTree(root, testBudget())
	require.NoError(t, err)
	assert.Equal(t, StatusComplete, tree.Status)
	assert.GreaterOrEqual(t, tree.FileCount, 2)
	assert.Contains(t, tree.EmptyDirs, filepath.ToSlash(filepath.Join("sub", "emptydir")), "empty directories are recorded explicitly")

	byPath := map[string]TreeEntry{}
	for _, e := range tree.Entries {
		byPath[e.Path] = e
	}
	a := byPath["a.txt"]
	require.NotNil(t, a.Content)
	assert.Equal(t, sha256Hex(t, []byte("alpha")), a.Content.Value)
	if runtime.GOOS != "windows" {
		// Windows reports simulated modes (typically 0o666), so exact
		// permission bits are asserted on Unix only.
		assert.Equal(t, uint32(0o640), a.Mode)
	}

	link := byPath["link.txt"]
	assert.Equal(t, TreeEntrySymlink, link.Type)
	require.NotNil(t, link.WithinRoot)
	assert.True(t, *link.WithinRoot)

	escaping := byPath["escaping.txt"]
	assert.Equal(t, TreeEntrySymlink, escaping.Type)
	require.NotNil(t, escaping.WithinRoot)
	assert.False(t, *escaping.WithinRoot, "escaping symlink must be flagged, not followed")
	assert.Nil(t, escaping.Content, "symlink target content never enters a digest")

	// Determinism: rebuilding gives the identical root digest.
	tree2, err := BuildTree(root, testBudget())
	require.NoError(t, err)
	assert.Equal(t, tree.RootDigest.Value, tree2.RootDigest.Value)
}

func TestBuildTree_Budgets(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 5; i++ {
		require.NoError(t, os.WriteFile(filepath.Join(root, string(rune('a'+i))+".txt"), []byte("12345"), 0o644))
	}
	// Five files of five bytes each: a ten-byte budget is crossed.
	_, err := BuildTree(root, Budget{MaxBytes: 10, MaxFiles: 1000, MaxTempDiskBytes: 100})
	require.ErrorIs(t, err, ErrBudgetExceeded)

	_, err = BuildTree(root, Budget{MaxBytes: 100, MaxFiles: 2, MaxTempDiskBytes: 100})
	require.ErrorIs(t, err, ErrBudgetExceeded)
}

func TestBuildTree_HardlinkHashedOnce(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("hardlink identity is not observable through os.FileInfo on Windows")
	}
	root := t.TempDir()
	primary := filepath.Join(root, "primary.txt")
	require.NoError(t, os.WriteFile(primary, []byte("hardlinked body"), 0o644))
	require.NoError(t, os.Link(primary, filepath.Join(root, "secondary.txt")))

	tree, err := BuildTree(root, testBudget())
	require.NoError(t, err)
	var hardlinks int
	for _, e := range tree.Entries {
		if e.Type == TreeEntryHardlink {
			hardlinks++
			assert.Equal(t, "primary.txt", e.HardlinkOf)
			require.NotNil(t, e.Content)
			assert.Equal(t, sha256Hex(t, []byte("hardlinked body")), e.Content.Value)
		}
	}
	assert.Equal(t, 1, hardlinks)
	assert.Equal(t, 1, tree.FileCount, "hardlinked bytes count once")
	assert.Equal(t, 1, tree.HardlinkCount)
}

// writeTarGz builds a small tar.gz archive in memory and returns its bytes.
func writeTarGz(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sortStrings(names)
	for _, name := range names {
		body := []byte(files[name])
		hdr := &tar.Header{Name: "root/" + name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write(body)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gzw.Close())
	return buf.Bytes()
}

func sortStrings(s []string) {
	for i := 0; i < len(s); i++ {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func TestSummarizeTarGz_TransportAndTreeOnePass(t *testing.T) {
	raw := writeTarGz(t, map[string]string{"a.txt": "alpha", "dir/b.txt": "beta-beta"})
	transport, tree, err := SummarizeTarGz(bytes.NewReader(raw), testBudget())
	require.NoError(t, err)

	// Transport digest is exactly the SHA-256 of the archived bytes.
	assert.Equal(t, sha256Hex(t, raw), transport.Value)
	assert.Equal(t, int64(len(raw)), transport.Bytes)

	// Tree digest covers the unpacked, normalized content instead.
	byPath := map[string]TreeEntry{}
	for _, e := range tree.Entries {
		byPath[e.Path] = e
	}
	a, ok := byPath["root/a.txt"]
	require.True(t, ok)
	require.NotNil(t, a.Content)
	assert.Equal(t, sha256Hex(t, []byte("alpha")), a.Content.Value)
	assert.NotEqual(t, transport.Value, tree.RootDigest.Value, "transport and tree digests must differ")
}

func TestSummarizeTarGz_PartialStreamNoReceipt(t *testing.T) {
	raw := writeTarGz(t, map[string]string{"a.txt": strings.Repeat("x", 4096)})
	// Truncate the stream mid-archive.
	_, _, err := SummarizeTarGz(bytes.NewReader(raw[:len(raw)/2]), testBudget())
	require.Error(t, err)
}

func TestSummarizePath_PlainFileAndZip(t *testing.T) {
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain.bin")
	require.NoError(t, os.WriteFile(plain, []byte("plain content"), 0o644))
	transport, tree, err := SummarizePath(plain, FormatNone, testBudget())
	require.NoError(t, err)
	assert.Equal(t, sha256Hex(t, []byte("plain content")), transport.Value)
	require.Len(t, tree.Entries, 1)
	assert.Equal(t, TreeEntryFile, tree.Entries[0].Type)

	// Zip: transport digest covers the archive file, tree covers the
	// uncompressed member content, and the two stay distinct.
	zipPath := filepath.Join(dir, "arch.zip")
	archive := writeZip(t, map[string]string{"z/a.txt": "zip-alpha"})
	require.NoError(t, os.WriteFile(zipPath, archive, 0o644))
	zipTransport, zipTree, err := SummarizePath(zipPath, FormatZip, testBudget())
	require.NoError(t, err)
	assert.Equal(t, sha256Hex(t, archive), zipTransport.Value)
	found := false
	for _, e := range zipTree.Entries {
		if e.Path == "z/a.txt" {
			found = true
			require.NotNil(t, e.Content)
			assert.Equal(t, sha256Hex(t, []byte("zip-alpha")), e.Content.Value)
		}
	}
	assert.True(t, found)
	assert.NotEqual(t, zipTransport.Value, zipTree.RootDigest.Value)
}

func writeZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, body := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)
		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestIsMultipartETag(t *testing.T) {
	assert.True(t, IsMultipartETag(`"abcdef0123456789abcdef0123456789-3"`))
	assert.False(t, IsMultipartETag(`"abcdef0123456789abcdef0123456789"`))
	assert.False(t, IsMultipartETag("mock-etag"))
}

func TestUnknownDigest(t *testing.T) {
	d := UnknownDigest()
	assert.Equal(t, Unknown, d.Algorithm)
	assert.Equal(t, Unknown, d.Value)
	assert.Equal(t, StatusUnknown, d.Status)
}

func testIdentity() Identity {
	return Identity{WorkflowName: "wf", WorkflowUID: "uid-1", NodeID: "node-1", NodeAttempt: 2, Template: TemplateID{Name: "tmpl"}}
}

func TestRecorder_FooterOnlyAfterFinalize(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "manifests")
	r, err := New(Config{Enabled: true, Dir: dir, Budget: testBudget()}, testIdentity(), DirectionOutput)
	require.NoError(t, err)

	require.NoError(t, r.Record(Entry{
		ArtifactName:    "out",
		Direction:       DirectionOutput,
		Template:        testIdentity().Template,
		Archive:         ArchiveRepresentation{Format: FormatNone, Compression: "none"},
		TransportDigest: Digest{Algorithm: "sha256", Value: "deadbeef", Bytes: 4, Status: StatusComplete},
		Storage:         StorageObject{Driver: "s3", Bucket: "b", Key: "k", VersionID: Unknown, ETag: Unknown, Size: -1, Status: StatusUnknown},
		Reexecuted:      true,
	}))

	// Before Finalize the persisted manifest exists but is not complete.
	data, err := os.ReadFile(r.Path())
	require.NoError(t, err)
	var partial Manifest
	require.NoError(t, json.Unmarshal(data, &partial))
	assert.False(t, partial.Footer.Complete)

	require.NoError(t, r.Finalize())
	data, err = os.ReadFile(r.Path())
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &partial))
	assert.True(t, partial.Footer.Complete)
	assert.Equal(t, 1, partial.Footer.EntryCount)
	assert.NotEmpty(t, partial.Footer.ManifestDigest)
	assert.Empty(t, partial.Footer.Signature, "no key configured: manifest stays unsigned")
	assert.Equal(t, 2, partial.NodeAttempt, "node attempt is recorded")
	assert.Equal(t, "uid-1", partial.WorkflowUID)

	// Finalize is idempotent.
	require.NoError(t, r.Finalize())
	// Record after finalize is rejected.
	require.ErrorIs(t, r.Record(Entry{ArtifactName: "late"}), ErrFinalized)
}

func TestRecorder_HMACSignatureOnlyWithKey(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "manifests")
	keyPath := filepath.Join(t.TempDir(), "key")
	require.NoError(t, os.WriteFile(keyPath, []byte("trusted-key"), 0o600))

	r, err := New(Config{Enabled: true, Dir: dir, Budget: testBudget(), SigningKeyPath: keyPath}, testIdentity(), DirectionInput)
	require.NoError(t, err)
	require.NoError(t, r.Record(Entry{ArtifactName: "in", Direction: DirectionInput, TransportDigest: UnknownDigest(), Storage: StorageObject{Driver: Unknown, VersionID: Unknown, ETag: Unknown, Size: -1, Status: StatusUnknown}}))
	require.NoError(t, r.Finalize())

	data, err := os.ReadFile(r.Path())
	require.NoError(t, err)
	var m Manifest
	require.NoError(t, json.Unmarshal(data, &m))
	assert.Equal(t, "HMAC-SHA256", m.Footer.SigningAlgorithm)
	require.NotEmpty(t, m.Footer.Signature)
	require.NoError(t, VerifySignature(m, []byte("trusted-key")))
	require.Error(t, VerifySignature(m, []byte("wrong-key")))
}

func TestRecorder_RejectsAbsoluteLocalPaths(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "manifests")
	r, err := New(Config{Enabled: true, Dir: dir, Budget: testBudget()}, testIdentity(), DirectionInput)
	require.NoError(t, err)
	// A TempDir path is absolute on every platform (drive letter on
	// Windows, leading slash on Unix).
	absolute := filepath.Join(t.TempDir(), "passwd")
	err = r.Record(Entry{
		ArtifactName:    "bad",
		Direction:       DirectionInput,
		TransportDigest: UnknownDigest(),
		Storage:         StorageObject{Driver: Unknown, VersionID: Unknown, ETag: Unknown, Size: -1, Status: StatusUnknown},
		Tree:            &TreeSummary{Entries: []TreeEntry{{Path: absolute, Type: TreeEntryFile}}},
	})
	require.Error(t, err)
}

func TestRecorder_TempDiskBudget(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "manifests")
	r, err := New(Config{Enabled: true, Dir: dir, Budget: testBudget()}, testIdentity(), DirectionInput)
	require.NoError(t, err)
	// Shrink the temp-disk budget below the size of a one-entry manifest.
	r.cfg.Budget.MaxTempDiskBytes = 1
	err = r.Record(Entry{ArtifactName: "in", Direction: DirectionInput, TransportDigest: UnknownDigest(), Storage: StorageObject{Driver: Unknown, VersionID: Unknown, ETag: Unknown, Size: -1, Status: StatusUnknown}})
	require.ErrorIs(t, err, ErrBudgetExceeded)
}
