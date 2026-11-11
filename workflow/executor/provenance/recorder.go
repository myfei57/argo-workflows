package provenance

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// signingAlgorithm is the only signature this package produces, and
// only when an explicit trusted key file is configured.
const signingAlgorithm = "HMAC-SHA256"

// ErrFinalized is returned when a recorder receives another Record
// after Finalize closed its manifest.
var ErrFinalized = errors.New("provenance: manifest already finalized")

// Recorder collects the artifact receipts for one task boundary
// (inputs or outputs). It is safe for the parallel plugin load stages:
// every mutation takes the mutex and persists atomically. A zero
// *Recorder is never constructed; the executor holds nil recorders
// when provenance is disabled, leaving the artifact path unchanged.
type Recorder struct {
	cfg      Config
	identity Identity
	direction Direction

	mu        sync.Mutex
	manifest  Manifest
	path      string
	finalized bool
}

// New builds a recorder and writes its empty, incomplete manifest so
// observers can see the boundary even before the first receipt lands.
func New(cfg Config, identity Identity, direction Direction) (*Recorder, error) {
	if !cfg.Enabled {
		return nil, errors.New("provenance: recorder constructed while disabled")
	}
	dir := cfg.Dir
	if dir == "" {
		return nil, errors.New("provenance: manifest directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("provenance: create manifest dir %s: %w", dir, err)
	}
	r := &Recorder{
		cfg:       cfg,
		identity:  identity,
		direction: direction,
		manifest: Manifest{
			SchemaVersion: SchemaVersion,
			WorkflowUID:   identity.WorkflowUID,
			WorkflowName:  identity.WorkflowName,
			NodeID:        identity.NodeID,
			NodeAttempt:   identity.NodeAttempt,
			Direction:     direction,
			Entries:       []Entry{},
			Footer:        Footer{Complete: false},
		},
	}
	r.path = filepath.Join(dir, fmt.Sprintf("%s-%s-attempt-%d.json", direction, safeName(identity.NodeID), identity.NodeAttempt))
	if err := r.persistLocked(); err != nil {
		return nil, err
	}
	return r, nil
}

// Path returns the manifest file location.
func (r *Recorder) Path() string {
	return r.path
}

// Record appends one receipt and persists. It is called only after the
// driver operation and every digest in e succeeded; a caller that hit a
// failed or partial stream simply does not call Record, so no success
// receipt can exist for it.
func (r *Recorder) Record(e Entry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finalized {
		return ErrFinalized
	}
	if err := validateEntry(e); err != nil {
		return err
	}
	if e.RecordedAt == "" {
		e.RecordedAt = nowRFC3339()
	}
	r.manifest.Entries = append(r.manifest.Entries, e)
	return r.persistLocked()
}

// Finalize closes the manifest: it freezes the entry set, computes the
// manifest digest over the canonical body and, only when a trusted key
// file is configured, attaches an HMAC signature. A manifest without
// Finalize keeps footer.complete=false and is not a success artifact.
// Finalize is idempotent: a later call returns the already-finalized
// manifest unchanged.
func (r *Recorder) Finalize() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.finalized {
		return nil
	}
	body, err := r.canonicalBodyLocked()
	if err != nil {
		return err
	}
	bodySum := sha256.Sum256(body)
	r.manifest.Footer = Footer{
		Complete:       true,
		EntryCount:     len(r.manifest.Entries),
		CompletedAt:    nowRFC3339(),
		ManifestDigest: hex.EncodeToString(bodySum[:]),
	}
	if r.cfg.SigningKeyPath != "" {
		key, err := os.ReadFile(r.cfg.SigningKeyPath)
		if err != nil {
			return fmt.Errorf("provenance: read signing key %s: %w", r.cfg.SigningKeyPath, err)
		}
		if len(strings.TrimSpace(string(key))) == 0 {
			return errors.New("provenance: signing key file is empty")
		}
		mac := hmac.New(sha256.New, key)
		_, _ = mac.Write(body)
		r.manifest.Footer.SigningAlgorithm = signingAlgorithm
		r.manifest.Footer.Signature = hex.EncodeToString(mac.Sum(nil))
	}
	r.finalized = true
	return r.persistLocked()
}

// canonicalBodyLocked renders everything except the footer as stable
// JSON. The footer digest and signature cover exactly this body.
func (r *Recorder) canonicalBodyLocked() ([]byte, error) {
	body := struct {
		SchemaVersion string    `json:"schemaVersion"`
		WorkflowUID   string    `json:"workflowUID"`
		WorkflowName  string    `json:"workflowName,omitempty"`
		NodeID        string    `json:"nodeID"`
		NodeAttempt   int       `json:"nodeAttempt"`
		Direction     Direction `json:"direction"`
		Entries       []Entry   `json:"entries"`
	}{
		SchemaVersion: r.manifest.SchemaVersion,
		WorkflowUID:   r.manifest.WorkflowUID,
		WorkflowName:  r.manifest.WorkflowName,
		NodeID:        r.manifest.NodeID,
		NodeAttempt:   r.manifest.NodeAttempt,
		Direction:     r.manifest.Direction,
		Entries:       r.manifest.Entries,
	}
	return json.MarshalIndent(body, "", "  ")
}

// persistLocked writes the current manifest via a same-directory temp
// file and rename, so a reader never observes a half-written JSON.
func (r *Recorder) persistLocked() error {
	data, err := json.MarshalIndent(r.manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("provenance: marshal manifest: %w", err)
	}
	if r.cfg.Budget.MaxTempDiskBytes > 0 && int64(len(data)) > r.cfg.Budget.MaxTempDiskBytes {
		return fmt.Errorf("%w: manifest %d bytes larger than temp budget %d", ErrBudgetExceeded, len(data), r.cfg.Budget.MaxTempDiskBytes)
	}
	tmp, err := os.CreateTemp(r.cfg.Dir, ".manifest-*.tmp")
	if err != nil {
		return fmt.Errorf("provenance: create temp manifest: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("provenance: write manifest: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("provenance: close manifest: %w", err)
	}
	if err := os.Rename(tmpName, r.path); err != nil {
		cleanup()
		return fmt.Errorf("provenance: finalize manifest rename: %w", err)
	}
	return nil
}

// validateEntry enforces the no-secrets / no-local-paths invariant on
// everything that reaches disk. Tree paths are relative by construction
// and storage objects hold coordinates only; this is the defense in
// depth check that keeps it true.
func validateEntry(e Entry) error {
	if e.ArtifactName == "" {
		return errors.New("provenance: entry missing artifact name")
	}
	if e.Tree != nil {
		for _, te := range e.Tree.Entries {
			if filepath.IsAbs(te.Path) || strings.HasPrefix(te.Path, "/") {
				return fmt.Errorf("provenance: absolute local path not permitted in tree entry %q", te.Path)
			}
			if te.LinkTarget != "" && filepath.IsAbs(te.LinkTarget) {
				// Absolute symlink targets are local-machine paths by
				// definition; the protocol allows relative, root-bounded
				// targets only.
				return fmt.Errorf("provenance: absolute symlink target not permitted in tree entry %q", te.Path)
			}
			if te.HardlinkOf != "" && (filepath.IsAbs(te.HardlinkOf) || strings.HasPrefix(te.HardlinkOf, "/")) {
				return fmt.Errorf("provenance: absolute hardlink reference not permitted in %q", te.Path)
			}
		}
	}
	return nil
}

// safeName reduces a node ID to filesystem-safe manifest name segment.
func safeName(nodeID string) string {
	if nodeID == "" {
		return "unknown-node"
	}
	var b strings.Builder
	for _, r := range nodeID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "unknown-node"
	}
	return b.String()
}

// VerifySignature checks an HMAC-SHA256 manifest signature against key
// and the manifest's canonical body. It exists for tests and downstream
// verifiers; the executor itself only signs with an explicitly provided
// trusted key.
func VerifySignature(m Manifest, key []byte) error {
	if m.Footer.SigningAlgorithm != signingAlgorithm {
		return errors.New("provenance: manifest is not signed")
	}
	body := struct {
		SchemaVersion string    `json:"schemaVersion"`
		WorkflowUID   string    `json:"workflowUID"`
		WorkflowName  string    `json:"workflowName,omitempty"`
		NodeID        string    `json:"nodeID"`
		NodeAttempt   int       `json:"nodeAttempt"`
		Direction     Direction `json:"direction"`
		Entries       []Entry   `json:"entries"`
	}{
		SchemaVersion: m.SchemaVersion,
		WorkflowUID:   m.WorkflowUID,
		WorkflowName:  m.WorkflowName,
		NodeID:        m.NodeID,
		NodeAttempt:   m.NodeAttempt,
		Direction:     m.Direction,
		Entries:       m.Entries,
	}
	data, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		return err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	want := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(want), []byte(m.Footer.Signature)) {
		return errors.New("provenance: signature mismatch")
	}
	return nil
}
