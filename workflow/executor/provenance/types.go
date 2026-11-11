// Package provenance produces optional artifact provenance manifests at
// the executor's real load/save success boundaries. A manifest is a
// reference graph of the artifacts one task actually moved: each entry
// names the storage coordinate, the transport archive representation,
// the digest of the exact bytes transferred and a digest of the
// normalized, unpacked file tree.
//
// Security and honesty invariants this package is built around:
//
//   - A success receipt exists only after the driver call and every digest
//     it references completed. A failed or partially read stream never
//     produces a success entry.
//   - Digests are computed in one pass over a local file or stream. The
//     package never calls a side-effecting Save/SaveStream twice and
//     never retries a consumed reader.
//   - A storage ETag (including a multipart upload ETag) is recorded as
//     an ETag, never as a content SHA.
//   - Manifests carry storage coordinates and relative tree paths only:
//     no credentials, no Secret values, no absolute local filesystem
//     paths. Signatures are attached only when an explicit trusted key
//     is configured; an unsigned digest is an internal integrity record,
//     not a publisher source attestation.
//   - Anything the storage backend does not expose (object version,
//     ETag) is recorded as Unknown rather than reconstructed or guessed.
package provenance

import "time"

// SchemaVersion is the manifest format version produced by this package.
const SchemaVersion = "argo-provenance/v1"

// Unknown is the explicit marker for a value the executor could not
// observe. It takes the place of a digest or a storage version so a
// missing fact is never rendered as a fabricated one.
const Unknown = "unknown"

// Digest statuses.
const (
	// StatusComplete means every byte was read and the digest is exact.
	StatusComplete = "complete"
	// StatusUnknown means the digest could not be computed; Value is
	// Unknown and the entry must not be read as proof of content.
	StatusUnknown = "unknown"
)

// Direction marks which side of a task's boundary an entry records.
type Direction string

const (
	DirectionInput  Direction = "input"
	DirectionOutput Direction = "output"
)

// Node entry types in a normalized file tree.
const (
	TreeEntryFile     = "file"
	TreeEntryDir      = "dir"
	TreeEntrySymlink  = "symlink"
	TreeEntryHardlink = "hardlink"
)

// Budget bounds the work a manifest build performs. Every limit is
// surfaced in the produced manifest so the reader knows under which
// constraints the digests were produced. The streaming implementation
// never spools whole artifacts into memory or into extra temp files, so
// MaxTempDiskBytes bounds only the atomic write of the manifest itself.
type Budget struct {
	// MaxBytes bounds the total regular-file content bytes hashed for
	// one tree. Exceeding it fails the build before any receipt is
	// written.
	MaxBytes int64 `json:"maxBytes"`
	// MaxFiles bounds the number of tree entries visited for one tree.
	MaxFiles int `json:"maxFiles"`
	// MaxTempDiskBytes bounds the temp space the build may use. The
	// implementation streams hashes, so only the atomic manifest write
	// consumes temp space (bounded by the manifest entry count, not
	// artifact size).
	MaxTempDiskBytes int64 `json:"maxTempDiskBytes"`
}

// DefaultBudget values used when the operator enables provenance
// without overriding a limit. They are generous rather than unlimited:
// an unbounded walk is indistinguishable from no budget.
func DefaultBudget() Budget {
	return Budget{
		MaxBytes:         100 * 1024 * 1024 * 1024, // 100 GiB of hashed content per tree
		MaxFiles:         100_000,
		MaxTempDiskBytes: 64 * 1024 * 1024, // 64 MiB for the manifest's atomic write
	}
}

// Config is the process-wide provenance configuration parsed from the
// executor environment. The zero value (Enabled == false) means no
// recorder is ever built and artifact processing keeps its original
// cost: no extra reads, no extra driver calls, no manifest files.
type Config struct {
	Enabled bool
	// Dir receives the completed manifest files. Defaults at wiring time
	// to <task scratch>/provenance.
	Dir string
	// Budget bounds every per-artifact tree build.
	Budget Budget
	// SigningKeyPath names a file holding an HMAC-SHA256 key. When empty,
	// manifests are never signed and explicitly say so. The key is read
	// once per finalized manifest and never written into one.
	SigningKeyPath string
}

// Identity identifies the task a manifest belongs to. It comes from
// controller-supplied executor state, never from artifact content.
type Identity struct {
	WorkflowName string
	WorkflowUID  string
	NodeID       string
	NodeAttempt  int
	Template     TemplateID
}

// TemplateID names the template a task ran.
type TemplateID struct {
	Name string `json:"name"`
}

// Digest is one content digest with the exact byte count it covers.
type Digest struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
	Bytes     int64  `json:"bytes"`
	Status    string `json:"status"`
}

// ArchiveRepresentation describes the transport representation of the
// bytes actually loaded or saved, as distinct from the unpacked tree.
type ArchiveRepresentation struct {
	// Format is "tar", "tar+gzip", "zip" or "none".
	Format string `json:"format"`
	// Compression is "gzip" or "none".
	Compression string `json:"compression"`
}

// StorageObject is the credential-free coordinate of the backend
// object. Version fields default to Unknown: the executor reports what
// the driver exposes and invent nothing.
type StorageObject struct {
	Driver    string `json:"driver"`
	Bucket    string `json:"bucket,omitempty"`
	Key       string `json:"key,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
	VersionID string `json:"versionID"`
	// ETag is the backend's opaque ETag verbatim. ETagMultipart marks
	// the composite ETag a multipart upload produces, which is not a
	// content SHA and must never be compared as one.
	ETag          string `json:"etag"`
	ETagMultipart bool   `json:"etagMultipart,omitempty"`
	// Size is the backend-reported object size when available; -1 means
	// unreported.
	Size int64 `json:"size"`
	// Status is "known" when a driver exposed object metadata, else
	// "unknown".
	Status string `json:"status"`
}

// TreeEntry is one normalized node of an unpacked artifact tree.
type TreeEntry struct {
	// Path is the slash-separated path relative to the artifact root;
	// the root itself is ".".
	Path string `json:"path"`
	// Type is file, dir, symlink or hardlink.
	Type string `json:"type"`
	// Mode holds the permission bits (os.FileMode.Perm), octal rendered
	// into the canonical digest line.
	Mode uint32 `json:"mode"`
	// Size is the regular-file size in bytes; zero for other types.
	Size int64 `json:"size,omitempty"`
	// Content is the per-file content digest for regular files.
	Content *Digest `json:"content,omitempty"`
	// LinkTarget is the literal symlink target as stored (never
	// resolved through content outside the root).
	LinkTarget string `json:"linkTarget,omitempty"`
	// WithinRoot reports whether LinkTarget stays inside the artifact
	// root without following it. An escaping symlink is recorded, not
	// traversed: its target content never enters any digest.
	WithinRoot *bool `json:"withinRoot,omitempty"`
	// HardlinkOf names the primary entry a hardlink entry shares
	// content with, so the content is hashed exactly once.
	HardlinkOf string `json:"hardlinkOf,omitempty"`
}

// TreeSummary is the normalized view of an unpacked artifact.
type TreeSummary struct {
	// RootDigest covers the canonical entry lines in walk order.
	RootDigest Digest      `json:"rootDigest"`
	Entries    []TreeEntry `json:"entries"`
	FileCount  int         `json:"fileCount"`
	DirCount   int         `json:"dirCount"`
	SymlinkCount int       `json:"symlinkCount"`
	HardlinkCount int      `json:"hardlinkCount"`
	// TotalBytes counts regular-file bytes once, at the primary entry
	// of hardlinked content.
	TotalBytes int64 `json:"totalBytes"`
	// EmptyDirs lists directory entries that contain no children. They
	// are recorded explicitly so an unpacked tree that contains them
	// has the same digest as one re-packed from the manifest.
	EmptyDirs []string `json:"emptyDirs"`
	Budget    Budget   `json:"budget"`
	Status    string   `json:"status"`
}

// Entry is one artifact receipt inside a manifest.
type Entry struct {
	ArtifactName string                `json:"artifactName"`
	Direction    Direction             `json:"direction"`
	Template     TemplateID            `json:"template"`
	Archive      ArchiveRepresentation `json:"archive"`
	// TransportDigest is the SHA-256 of the exact archive bytes the
	// driver loaded or saved. It is distinct from Tree.RootDigest.
	TransportDigest Digest `json:"transportDigest"`
	// Tree is the normalized unpacked-tree summary.
	Tree *TreeSummary `json:"tree,omitempty"`
	// Storage is the credential-free backend coordinate and version.
	Storage StorageObject `json:"storage"`
	// CacheHit is true only when content was served from a cache
	// without this task moving bytes. The executor's own load/save path
	// always moves bytes, so this stays false unless an outer layer
	// reports a hit; a hit with no observable digest leaves that digest
	// Unknown rather than claiming a re-execution.
	CacheHit bool `json:"cacheHit"`
	// Reexecuted affirms this receipt observes an actual driver
	// transfer performed by this task.
	Reexecuted bool `json:"reexecuted"`
	RecordedAt string `json:"recordedAt"`
}

// Footer closes a manifest. Complete is false while entries are still
// being appended and flips true only on Finalize, after which the entry
// set, manifest digest and optional signature are immutable.
type Footer struct {
	Complete       bool   `json:"complete"`
	EntryCount     int    `json:"entryCount"`
	CompletedAt    string `json:"completedAt,omitempty"`
	ManifestDigest string `json:"manifestDigest,omitempty"`
	// SigningAlgorithm is "HMAC-SHA256" only when an explicit trusted
	// key file is configured. Empty means the manifest is unsigned and
	// its digests must not be treated as publisher attestations.
	SigningAlgorithm string `json:"signingAlgorithm,omitempty"`
	Signature        string `json:"signature,omitempty"`
}

// Manifest is the reference graph artifact for one task boundary.
type Manifest struct {
	SchemaVersion string    `json:"schemaVersion"`
	WorkflowUID   string    `json:"workflowUID"`
	WorkflowName  string    `json:"workflowName,omitempty"`
	NodeID        string    `json:"nodeID"`
	NodeAttempt   int       `json:"nodeAttempt"`
	Direction     Direction `json:"direction"`
	Entries       []Entry   `json:"entries"`
	Footer        Footer    `json:"footer"`
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
