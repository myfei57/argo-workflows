package executor

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
	artifactcommon "github.com/argoproj/argo-workflows/v4/workflow/artifacts/common"
	"github.com/argoproj/argo-workflows/v4/workflow/artifacts/resource"
	"github.com/argoproj/argo-workflows/v4/workflow/executor/provenance"
	"github.com/argoproj/argo-workflows/v4/workflow/executor/tracing"
	wfutil "github.com/argoproj/argo-workflows/v4/workflow/util"
)

// fakeArtifactDriver counts every driver call so tests can prove
// provenance adds no second Save/Load and never touches SaveStream.
type fakeArtifactDriver struct {
	mu              sync.Mutex
	loadCalls       int
	saveCalls       int
	saveStreamCalls int
	loadContent     []byte // written into the destination on Load
	loadErr         error
	saveErr         error
	withObjectInfo  bool
}

func (f *fakeArtifactDriver) Load(_ context.Context, _ *wfv1.Artifact, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadCalls++
	if f.loadErr != nil {
		// Simulate a partial write before failure: a failed stream must
		// not leave a success receipt behind.
		if len(f.loadContent) > 0 {
			_ = os.WriteFile(path, f.loadContent[:len(f.loadContent)/2], 0o644)
		}
		return f.loadErr
	}
	return os.WriteFile(path, f.loadContent, 0o644)
}

func (f *fakeArtifactDriver) OpenStream(_ context.Context, _ *wfv1.Artifact) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.loadContent)), nil
}

func (f *fakeArtifactDriver) Save(_ context.Context, _ string, _ *wfv1.Artifact) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveCalls++
	if f.saveErr != nil {
		return f.saveErr
	}
	return nil
}

func (f *fakeArtifactDriver) SaveStream(_ context.Context, _ io.Reader, _ *wfv1.Artifact) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveStreamCalls++
	return errors.New("SaveStream must not be used by the executor provenance path")
}

func (f *fakeArtifactDriver) Delete(context.Context, *wfv1.Artifact) error {
	return artifactcommon.ErrDeleteNotSupported
}
func (f *fakeArtifactDriver) ListObjects(context.Context, *wfv1.Artifact) ([]string, error) {
	return nil, nil
}
func (f *fakeArtifactDriver) IsDirectory(context.Context, *wfv1.Artifact) (bool, error) {
	return false, nil
}

func (f *fakeArtifactDriver) ObjectInfo(_ context.Context, _ *wfv1.Artifact) (*artifactcommon.ObjectInfo, error) {
	if !f.withObjectInfo {
		return nil, errors.New("not implemented")
	}
	return &artifactcommon.ObjectInfo{
		VersionID: "version-7",
		// Composite multipart shape: must be flagged and never treated
		// as a content SHA.
		ETag: "abcdef0123456789abcdef0123456789-2",
		Size: 1234,
	}, nil
}

func (f *fakeArtifactDriver) counts() (int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loadCalls, f.saveCalls, f.saveStreamCalls
}

func newProvenanceTestExecutor(t *testing.T, driver *fakeArtifactDriver, cfg *provenance.Config, attempt int) *WorkflowExecutor {
	t.Helper()
	ctx := logging.TestContext(t.Context())
	tr, err := tracing.New(ctx, `argoexec`)
	require.NoError(t, err)

	root := t.TempDir()
	paths := PodPaths()
	paths.Scratch = filepath.Join(root, "scratch")
	paths.MainFS = filepath.Join(root, "mainfs")
	paths.InputArtifacts = filepath.Join(root, "inputs")
	require.NoError(t, os.MkdirAll(paths.Scratch, 0o755))
	require.NoError(t, os.MkdirAll(paths.MainFS, 0o755))
	require.NoError(t, os.MkdirAll(paths.InputArtifacts, 0o755))

	return &WorkflowExecutor{
		paths:       paths,
		nodeAttempt: attempt,
		nodeID:      "node-x",
		workflow:    "wf",
		workflowUID: "wf-uid",
		Process: &Process{
			Tracing:       tr,
			provenanceCfg: cfg,
			newDriver: func(_ context.Context, _ *wfv1.Artifact, _ resource.Interface) (artifactcommon.ArtifactDriver, error) {
				return driver, nil
			},
		},
	}
}

func s3OutputArtifact(name, mountPath, key string) wfv1.Artifact {
	return wfv1.Artifact{
		Name: name,
		Path: mountPath,
		ArtifactLocation: wfv1.ArtifactLocation{
			S3: &wfv1.S3Artifact{
				S3Bucket: wfv1.S3Bucket{Endpoint: "http://minio:9000", Bucket: "bucket"},
				Key:      key,
			},
		},
	}
}

func readManifest(t *testing.T, path string) provenance.Manifest {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var m provenance.Manifest
	require.NoError(t, json.Unmarshal(data, &m))
	return m
}

// TestProvenance_OutputUploadSuccess drives SaveArtifacts with a real
// directory tree (files, empty dir and symlink), then checks the receipt
// matches the single driver Save one-to-one.
func TestProvenance_OutputUploadSuccess(t *testing.T) {
	driver := &fakeArtifactDriver{withObjectInfo: true}
	cfg := &provenance.Config{Enabled: true, Budget: provenance.DefaultBudget()}
	we := newProvenanceTestExecutor(t, driver, cfg, 1)

	mountPath := "/data/out"
	source := filepath.Join(we.paths.MainFS, mountPath)
	require.NoError(t, os.MkdirAll(filepath.Join(source, "emptydir"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(source, "sub"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(source, "a.txt"), []byte("alpha"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(source, "sub", "b.txt"), []byte("beta-beta"), 0o755))
	require.NoError(t, os.Symlink(filepath.Join("sub", "b.txt"), filepath.Join(source, "link.txt")))

	art := s3OutputArtifact("out-art", mountPath, "key/out-art.tgz")
	we.Template = wfv1.Template{
		Container: &apiv1.Container{
			VolumeMounts: []apiv1.VolumeMount{{Name: "vol", MountPath: "/data"}},
		},
		Outputs: wfv1.Outputs{Artifacts: []wfv1.Artifact{art}},
	}

	ctx := logging.TestContext(t.Context())
	saved, err := we.SaveArtifacts(ctx)
	require.NoError(t, err)
	require.Len(t, saved, 1)
	we.finalizeOutputProvenance(ctx)

	loadCalls, saveCalls, streamCalls := driver.counts()
	assert.Equal(t, 0, loadCalls)
	assert.Equal(t, 1, saveCalls, "exactly one Save, provenance must not duplicate it")
	assert.Equal(t, 0, streamCalls, "SaveStream is never used by this path")

	manifestPath := filepath.Join(we.paths.Scratch, "provenance", "output-node-x-attempt-1.json")
	m := readManifest(t, manifestPath)
	require.True(t, m.Footer.Complete)
	require.Len(t, m.Entries, 1, "one receipt for one driver Save")
	entry := m.Entries[0]
	assert.Equal(t, provenance.DirectionOutput, entry.Direction)
	assert.Equal(t, "out-art", entry.ArtifactName)
	assert.Equal(t, 1, m.NodeAttempt)
	assert.Equal(t, "wf-uid", m.WorkflowUID)
	assert.Equal(t, provenance.StatusComplete, entry.TransportDigest.Status)
	assert.Equal(t, "tar+gzip", entry.Archive.Format)
	assert.Equal(t, "gzip", entry.Archive.Compression)

	require.NotNil(t, entry.Tree)
	treePaths := map[string]provenance.TreeEntry{}
	for _, te := range entry.Tree.Entries {
		treePaths[te.Path] = te
	}
	// The tar root is the source directory's base name ("out").
	assert.Contains(t, treePaths, "out/a.txt")
	assert.Contains(t, treePaths, "out/link.txt")
	link := treePaths["out/link.txt"]
	assert.Equal(t, provenance.TreeEntrySymlink, link.Type)
	require.NotNil(t, link.WithinRoot)
	assert.True(t, *link.WithinRoot)
	assert.Contains(t, entry.Tree.EmptyDirs, "out/emptydir")

	// Storage coordinates carry no secrets and keep ETag facts honest.
	assert.Equal(t, "s3", entry.Storage.Driver)
	assert.Equal(t, "bucket", entry.Storage.Bucket)
	assert.Equal(t, "key/out-art.tgz", entry.Storage.Key)
	assert.Equal(t, "version-7", entry.Storage.VersionID)
	assert.True(t, entry.Storage.ETagMultipart)
	assert.NotEqual(t, entry.Storage.ETag, entry.TransportDigest.Value, "an ETag is never a content SHA")
	assert.True(t, entry.Reexecuted)
	assert.False(t, entry.CacheHit)
}

// TestProvenance_DisabledHasNoOverhead proves the default-off path
// creates no manifest and performs exactly the unmodified driver calls.
func TestProvenance_DisabledHasNoOverhead(t *testing.T) {
	driver := &fakeArtifactDriver{}
	we := newProvenanceTestExecutor(t, driver, nil, 0)

	mountPath := "/data/out"
	source := filepath.Join(we.paths.MainFS, mountPath)
	require.NoError(t, os.MkdirAll(source, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(source, "a.txt"), []byte("alpha"), 0o644))
	art := s3OutputArtifact("out-art", mountPath, "key/out-art.tgz")
	we.Template = wfv1.Template{
		Container: &apiv1.Container{VolumeMounts: []apiv1.VolumeMount{{Name: "vol", MountPath: "/data"}}},
		Outputs:   wfv1.Outputs{Artifacts: []wfv1.Artifact{art}},
	}

	ctx := logging.TestContext(t.Context())
	_, err := we.SaveArtifacts(ctx)
	require.NoError(t, err)

	_, saveCalls, streamCalls := driver.counts()
	assert.Equal(t, 1, saveCalls)
	assert.Equal(t, 0, streamCalls)
	_, statErr := os.Stat(filepath.Join(we.paths.Scratch, "provenance"))
	assert.True(t, os.IsNotExist(statErr), "no provenance directory while disabled")
	assert.Nil(t, we.outputProvenance)
}

// makeTarGz builds an archive with one top-level directory holding files.
func makeTarGz(t *testing.T) []byte {
	t.Helper()
	files := map[string]string{
		"myart/a.txt":     "alpha",
		"myart/sub/b.txt": "beta-beta",
	}
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)
	for name, body := range files {
		hdr := &tar.Header{Name: name, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}
		require.NoError(t, tw.WriteHeader(hdr))
		_, err := tw.Write([]byte(body))
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	require.NoError(t, gzw.Close())
	return buf.Bytes()
}

// TestProvenance_InputLoadSuccess proves an input receipt records the
// transport digest of the downloaded bytes and the unpacked tree, with
// exactly one Load call.
func TestProvenance_InputLoadSuccess(t *testing.T) {
	driver := &fakeArtifactDriver{loadContent: makeTarGz(t)}
	cfg := &provenance.Config{Enabled: true, Budget: provenance.DefaultBudget()}
	we := newProvenanceTestExecutor(t, driver, cfg, 2)

	art := wfv1.Artifact{
		Name: "in-art",
		Path: filepath.Join(we.paths.InputArtifacts, "in-art"),
		ArtifactLocation: wfv1.ArtifactLocation{
			S3: &wfv1.S3Artifact{
				S3Bucket: wfv1.S3Bucket{Endpoint: "http://minio:9000", Bucket: "bucket"},
				Key:      "key/in-art.tgz",
			},
		},
	}
	we.Template = wfv1.Template{Inputs: wfv1.Inputs{Artifacts: []wfv1.Artifact{art}}}

	ctx := logging.TestContext(t.Context())
	require.NoError(t, we.loadArtifacts(ctx, ""))
	// The plan runs finalize-input-provenance after the load stages;
	// the method-level test invokes the same finalization explicitly.
	we.finalizeInputProvenance(ctx)

	loadCalls, _, streamCalls := driver.counts()
	assert.Equal(t, 1, loadCalls, "exactly one Load; digesting reads the local temp file only")
	assert.Equal(t, 0, streamCalls)

	manifestPath := filepath.Join(we.paths.Scratch, "provenance", "input-node-x-attempt-2.json")
	m := readManifest(t, manifestPath)
	require.True(t, m.Footer.Complete)
	require.Len(t, m.Entries, 1)
	entry := m.Entries[0]
	assert.Equal(t, provenance.DirectionInput, entry.Direction)
	assert.Equal(t, provenance.StatusComplete, entry.TransportDigest.Status)
	assert.Equal(t, "tar+gzip", entry.Archive.Format)
	require.NotNil(t, entry.Tree)
	treePaths := map[string]bool{}
	for _, te := range entry.Tree.Entries {
		treePaths[te.Path] = true
	}
	// The archive's single top-level dir is renamed to the artifact
	// path, so the tree root contains its children directly.
	assert.True(t, treePaths["a.txt"])
	// No ObjectInfoDriver data: version facts stay Unknown.
	assert.Equal(t, provenance.StatusUnknown, entry.Storage.Status)
	assert.Equal(t, provenance.Unknown, entry.Storage.VersionID)
	assert.Equal(t, provenance.Unknown, entry.Storage.ETag)
	assert.Equal(t, 2, m.NodeAttempt, "retry attempt is recorded")
}

// TestProvenance_FailedLoadNoReceipt proves a failed/partial transfer
// never produces a success receipt.
func TestProvenance_FailedLoadNoReceipt(t *testing.T) {
	driver := &fakeArtifactDriver{
		loadErr:     errors.New("stream interrupted"),
		loadContent: []byte("partial-bytes-that-never-complete"),
	}
	cfg := &provenance.Config{Enabled: true, Budget: provenance.DefaultBudget()}
	we := newProvenanceTestExecutor(t, driver, cfg, 0)

	art := wfv1.Artifact{
		Name: "in-art",
		Path: filepath.Join(we.paths.InputArtifacts, "in-art"),
		ArtifactLocation: wfv1.ArtifactLocation{
			S3: &wfv1.S3Artifact{S3Bucket: wfv1.S3Bucket{Endpoint: "http://minio:9000", Bucket: "bucket"}, Key: "k"},
		},
	}
	we.Template = wfv1.Template{Inputs: wfv1.Inputs{Artifacts: []wfv1.Artifact{art}}}

	ctx := logging.TestContext(t.Context())
	err := we.loadArtifacts(ctx, "")
	require.Error(t, err)
	assert.Nil(t, we.inputProvenance, "no recorder exists when no artifact succeeded")
	matches, _ := filepath.Glob(filepath.Join(we.paths.Scratch, "provenance", "*.json"))
	assert.Empty(t, matches, "no success manifest for a failed boundary")
}

// TestProvenance_FailedSaveNoReceipt proves a failed upload leaves no
// output receipt even though staging succeeded.
func TestProvenance_FailedSaveNoReceipt(t *testing.T) {
	driver := &fakeArtifactDriver{saveErr: errors.New("upload denied")}
	cfg := &provenance.Config{Enabled: true, Budget: provenance.DefaultBudget()}
	we := newProvenanceTestExecutor(t, driver, cfg, 0)

	mountPath := "/data/out"
	source := filepath.Join(we.paths.MainFS, mountPath)
	require.NoError(t, os.MkdirAll(source, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(source, "a.txt"), []byte("alpha"), 0o644))
	art := s3OutputArtifact("out-art", mountPath, "key/out-art.tgz")
	we.Template = wfv1.Template{
		Container: &apiv1.Container{VolumeMounts: []apiv1.VolumeMount{{Name: "vol", MountPath: "/data"}}},
		Outputs:   wfv1.Outputs{Artifacts: []wfv1.Artifact{art}},
	}

	ctx := logging.TestContext(t.Context())
	_, err := we.SaveArtifacts(ctx)
	require.Error(t, err)
	_, saveCalls, _ := driver.counts()
	assert.Equal(t, 1, saveCalls, "the failed Save is not retried by provenance")
	matches, _ := filepath.Glob(filepath.Join(we.paths.Scratch, "provenance", "*.json"))
	assert.Empty(t, matches)
}

// TestProvenance_ParallelRecorderCreation mirrors the init-less plugin
// load stages: concurrent first access must produce one shared recorder.
func TestProvenance_ParallelRecorderCreation(t *testing.T) {
	driver := &fakeArtifactDriver{}
	cfg := &provenance.Config{Enabled: true, Budget: provenance.DefaultBudget()}
	we := newProvenanceTestExecutor(t, driver, cfg, 0)

	ctx := logging.TestContext(t.Context())
	const goroutines = 8
	results := make(chan *provenance.Recorder, goroutines)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- we.inputRecorder(ctx)
		}()
	}
	wg.Wait()
	close(results)
	first := <-results
	require.NotNil(t, first)
	for r := range results {
		assert.Same(t, first, r)
	}
}

// TestNodeAttemptFromName guards the controller-side parsing used to
// populate ARGO_NODE_ATTEMPT, covering retry attempt naming.
func TestNodeAttemptFromName(t *testing.T) {
	cases := map[string]int{
		"node":          0,
		"retry-node(0)": 0,
		"retry-node(1)": 1,
		"retry-node(7)": 7,
		"retry-node()":  0,
		"retry-node(x)": 0,
	}
	for name, want := range cases {
		assert.Equal(t, want, wfutil.NodeAttemptFromName(name), name)
	}
}
