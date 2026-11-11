package executor

import (
	"context"
	"strings"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
	artifactcommon "github.com/argoproj/argo-workflows/v4/workflow/artifacts/common"
	"github.com/argoproj/argo-workflows/v4/workflow/executor/provenance"
)

// This file wires the optional provenance manifest feature into the
// executor's real load/save boundaries. Every hook is a no-op unless the
// process was started with provenance enabled, so the default artifact
// path performs no extra reads, hashing or driver calls.
//
// Honesty rules enforced here:
//   - a receipt is recorded only after the driver call AND every digest
//     it contains succeeded; a failed or partial read simply skips the
//     record, it never produces a success entry;
//   - digests read local files/streams once. The storage driver is never
//     called twice for one artifact and a consumed reader is never
//     retried;
//   - backend metadata (version, ETag) is taken from an optional
//     ObjectInfoDriver. A driver without it, or a failed lookup, becomes
//     Unknown rather than a guessed value;
//   - only storage coordinates and relative tree paths are recorded.

// provenanceEnabled reports whether manifests are configured.
func (we *WorkflowExecutor) provenanceEnabled() bool {
	return we.Process != nil && we.provenanceCfg != nil && we.provenanceCfg.Enabled
}

func (we *WorkflowExecutor) provenanceBudget() provenance.Budget {
	if we.provenanceCfg != nil && we.provenanceCfg.Budget.MaxBytes > 0 {
		return we.provenanceCfg.Budget
	}
	return provenance.DefaultBudget()
}

func (we *WorkflowExecutor) provenanceIdentity() provenance.Identity {
	return provenance.Identity{
		WorkflowName: we.workflow,
		WorkflowUID:  string(we.workflowUID),
		NodeID:       we.nodeID,
		NodeAttempt:  we.nodeAttempt,
		Template:     provenance.TemplateID{Name: we.Template.Name},
	}
}

func (we *WorkflowExecutor) provenanceDir() string {
	if we.provenanceCfg != nil && we.provenanceCfg.Dir != "" {
		return we.provenanceCfg.Dir
	}
	return we.paths.Scratch + "/provenance"
}

func (we *WorkflowExecutor) provenanceConfig() *provenance.Config {
	if !we.provenanceEnabled() {
		return nil
	}
	cfg := *we.provenanceCfg
	cfg.Dir = we.provenanceDir()
	if cfg.Budget.MaxBytes == 0 {
		cfg.Budget = provenance.DefaultBudget()
	}
	return &cfg
}

// inputRecorder lazily creates the input-boundary recorder. A creation
// failure is logged and returns nil: provenance never blocks an actual
// artifact transfer. The mutex covers the parallel plugin load stages.
func (we *WorkflowExecutor) inputRecorder(ctx context.Context) *provenance.Recorder {
	we.provenanceMu.Lock()
	defer we.provenanceMu.Unlock()
	if we.inputProvenance != nil {
		return we.inputProvenance
	}
	if !we.provenanceEnabled() {
		return nil
	}
	recorder, err := provenance.New(*we.provenanceConfig(), we.provenanceIdentity(), provenance.DirectionInput)
	if err != nil {
		logging.RequireLoggerFromContext(ctx).WithError(err).Error(ctx, "failed to initialize input provenance recorder")
		return nil
	}
	we.inputProvenance = recorder
	return recorder
}

// outputRecorder lazily creates the output-boundary recorder.
func (we *WorkflowExecutor) outputRecorder(ctx context.Context) *provenance.Recorder {
	we.provenanceMu.Lock()
	defer we.provenanceMu.Unlock()
	if we.outputProvenance != nil {
		return we.outputProvenance
	}
	if !we.provenanceEnabled() {
		return nil
	}
	recorder, err := provenance.New(*we.provenanceConfig(), we.provenanceIdentity(), provenance.DirectionOutput)
	if err != nil {
		logging.RequireLoggerFromContext(ctx).WithError(err).Error(ctx, "failed to initialize output provenance recorder")
		return nil
	}
	we.outputProvenance = recorder
	return recorder
}

// finalizeInputProvenance closes the input manifest with a completion
// footer. It is called only when every input artifact loaded; an early
// return leaves footer.complete=false, so no success artifact exists.
func (we *WorkflowExecutor) finalizeInputProvenance(ctx context.Context) {
	if we.inputProvenance == nil {
		return
	}
	if err := we.inputProvenance.Finalize(); err != nil {
		logging.RequireLoggerFromContext(ctx).WithError(err).Error(ctx, "failed to finalize input provenance manifest")
	}
}

// finalizeOutputProvenance closes the output manifest with a completion
// footer, only after every output artifact saved successfully.
func (we *WorkflowExecutor) finalizeOutputProvenance(ctx context.Context) {
	if we.outputProvenance == nil {
		return
	}
	if err := we.outputProvenance.Finalize(); err != nil {
		logging.RequireLoggerFromContext(ctx).WithError(err).Error(ctx, "failed to finalize output provenance manifest")
	}
}

// detectInputArchiveFormat maps the artifact archive strategy (and the
// tarball auto-detection) onto the provenance transport format.
func (we *WorkflowExecutor) detectInputArchiveFormat(ctx context.Context, art wfv1.Artifact, tempArtPath string) (string, error) {
	switch {
	case art.GetArchive().None != nil:
		return provenance.FormatNone, nil
	case art.GetArchive().Zip != nil:
		return provenance.FormatZip, nil
	case art.GetArchive().Tar != nil:
		// The executor stages tar strategy artifacts as gzip-compressed
		// tgz streams; the downloaded bytes are tar+gzip.
		return provenance.FormatTarGzip, nil
	default:
		isTar, err := isTarball(ctx, tempArtPath)
		if err != nil {
			return "", err
		}
		if isTar {
			return provenance.FormatTarGzip, nil
		}
		return provenance.FormatNone, nil
	}
}

// outputArchiveFormat identifies the transport representation of the
// staged local file the driver is about to upload.
func outputArchiveFormat(localArtPath string) string {
	lower := strings.ToLower(localArtPath)
	switch {
	case strings.HasSuffix(lower, ".tgz"), strings.HasSuffix(lower, ".tar.gz"):
		return provenance.FormatTarGzip
	case strings.HasSuffix(lower, ".tar"):
		return provenance.FormatTar
	case strings.HasSuffix(lower, ".zip"):
		return provenance.FormatZip
	default:
		return provenance.FormatNone
	}
}

// recordInputArtifact writes one input receipt after Load, transport
// hashing and unarchive all succeeded. transportDigest is the digest of
// the exact downloaded bytes at tempArtPath; the tree is built from the
// extracted artPath without following symlinks.
func (we *WorkflowExecutor) recordInputArtifact(ctx context.Context, art, driverArt *wfv1.Artifact, driver artifactcommon.ArtifactDriver, artPath, archiveFormat string, transportDigest provenance.Digest) {
	logger := logging.RequireLoggerFromContext(ctx)
	recorder := we.inputRecorder(ctx)
	if recorder == nil {
		return
	}
	budget := we.provenanceBudget()
	tree, err := provenance.BuildTree(artPath, budget)
	if err != nil {
		logger.WithField("artifact", art.Name).WithError(err).Error(ctx, "failed to build input provenance tree; no receipt written")
		return
	}
	entry := provenance.Entry{
		ArtifactName:    art.Name,
		Direction:       provenance.DirectionInput,
		Template:        we.provenanceIdentity().Template,
		Archive:         archiveRepresentation(archiveFormat),
		TransportDigest: transportDigest,
		Tree:            tree,
		Storage:         storageObjectOf(ctx, driverArt, driver),
		CacheHit:        false,
		Reexecuted:      true,
	}
	if err := recorder.Record(entry); err != nil {
		logger.WithField("artifact", art.Name).WithError(err).Error(ctx, "failed to record input provenance entry")
	}
}

// recordOutputArtifact writes one output receipt after the driver Save
// succeeded. It summarizes the exact staged bytes that were uploaded and
// the normalized tree in one local pass per format, with no second Save
// or SaveStream call.
func (we *WorkflowExecutor) recordOutputArtifact(ctx context.Context, art, driverArt *wfv1.Artifact, driver artifactcommon.ArtifactDriver, localArtPath string) {
	logger := logging.RequireLoggerFromContext(ctx)
	recorder := we.outputRecorder(ctx)
	if recorder == nil {
		return
	}
	format := outputArchiveFormat(localArtPath)
	transportDigest, tree, err := provenance.SummarizePath(localArtPath, format, we.provenanceBudget())
	if err != nil {
		logger.WithField("artifact", art.Name).WithError(err).Error(ctx, "failed to summarize output artifact; no receipt written")
		return
	}
	entry := provenance.Entry{
		ArtifactName:    art.Name,
		Direction:       provenance.DirectionOutput,
		Template:        we.provenanceIdentity().Template,
		Archive:         archiveRepresentation(format),
		TransportDigest: transportDigest,
		Tree:            tree,
		Storage:         storageObjectOf(ctx, driverArt, driver),
		CacheHit:        false,
		Reexecuted:      true,
	}
	if err := recorder.Record(entry); err != nil {
		logger.WithField("artifact", art.Name).WithError(err).Error(ctx, "failed to record output provenance entry")
	}
}

func archiveRepresentation(format string) provenance.ArchiveRepresentation {
	switch format {
	case provenance.FormatTarGzip:
		return provenance.ArchiveRepresentation{Format: provenance.FormatTarGzip, Compression: "gzip"}
	case provenance.FormatTar:
		return provenance.ArchiveRepresentation{Format: provenance.FormatTar, Compression: "none"}
	case provenance.FormatZip:
		return provenance.ArchiveRepresentation{Format: provenance.FormatZip, Compression: "none"}
	default:
		return provenance.ArchiveRepresentation{Format: provenance.FormatNone, Compression: "none"}
	}
}

// storageObjectOf builds the credential-free storage coordinate and
// fills backend version metadata when the driver optionally exposes it.
// Any unavailable fact stays Unknown; it is never reconstructed.
func storageObjectOf(ctx context.Context, art *wfv1.Artifact, driver artifactcommon.ArtifactDriver) provenance.StorageObject {
	so := storageCoordinates(art)
	infoDriver, ok := driver.(artifactcommon.ObjectInfoDriver)
	if !ok {
		return unknownStorage(so)
	}
	info, err := infoDriver.ObjectInfo(ctx, art)
	if err != nil || info == nil {
		logging.RequireLoggerFromContext(ctx).WithField("artifact", art.Name).WithError(err).Debug(ctx, "object metadata unavailable; recording Unknown")
		return unknownStorage(so)
	}
	so.Status = "known"
	so.VersionID = provenance.Unknown
	if info.VersionID != "" {
		so.VersionID = info.VersionID
	}
	so.ETag = provenance.Unknown
	if info.ETag != "" {
		so.ETag = info.ETag
		so.ETagMultipart = provenance.IsMultipartETag(info.ETag)
	}
	so.Size = info.Size
	return so
}

func unknownStorage(so provenance.StorageObject) provenance.StorageObject {
	so.VersionID = provenance.Unknown
	so.ETag = provenance.Unknown
	so.Size = -1
	so.Status = provenance.StatusUnknown
	return so
}

// storageCoordinates extracts non-secret location fields only. Secret
// selectors, tokens and keys are deliberately never read here.
func storageCoordinates(art *wfv1.Artifact) provenance.StorageObject {
	switch {
	case art.S3 != nil:
		return provenance.StorageObject{Driver: "s3", Bucket: art.S3.Bucket, Key: art.S3.Key, Endpoint: art.S3.Endpoint}
	case art.GCS != nil:
		return provenance.StorageObject{Driver: "gcs", Bucket: art.GCS.Bucket, Key: art.GCS.Key}
	case art.OSS != nil:
		return provenance.StorageObject{Driver: "oss", Bucket: art.OSS.Bucket, Key: art.OSS.Key, Endpoint: art.OSS.Endpoint}
	case art.Azure != nil:
		return provenance.StorageObject{Driver: "azure", Bucket: art.Azure.Container, Key: art.Azure.Blob, Endpoint: art.Azure.Endpoint}
	case art.HDFS != nil:
		return provenance.StorageObject{Driver: "hdfs", Key: art.HDFS.Path}
	case art.HTTP != nil:
		return provenance.StorageObject{Driver: "http", Endpoint: art.HTTP.URL}
	case art.Artifactory != nil:
		return provenance.StorageObject{Driver: "artifactory", Endpoint: art.Artifactory.URL}
	case art.Git != nil:
		return provenance.StorageObject{Driver: "git", Endpoint: art.Git.Repo, Key: art.Git.Revision}
	case art.Raw != nil:
		return provenance.StorageObject{Driver: "raw"}
	case art.Plugin != nil:
		return provenance.StorageObject{Driver: "plugin", Key: art.Plugin.Key}
	default:
		return provenance.StorageObject{Driver: provenance.Unknown}
	}
}
