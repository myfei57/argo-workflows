package artifactgc

import (
	"context"
	"fmt"
)

// maxAnnotationBytes bounds a Plan serialized into a Kubernetes annotation. The
// configured MaxObjects/MaxBytes limits keep Plans below this in practice.
const maxAnnotationBytes = 256 * 1024

// PlanStore persists Plans across controller and Pod restarts. Implementations must
// not store credentials: Plan documents only contain non-secret coordinates.
type PlanStore interface {
	// Load reads a previously frozen Plan. A missing Plan returns (nil, nil).
	Load(ctx context.Context, key string) (*ArtifactGCPlan, error)
	// Save writes a Plan, rejecting documents that exceed the persistence bound.
	Save(ctx context.Context, key string, plan *ArtifactGCPlan) error
}

// EncodePlan serializes a Plan and enforces the persistence size bound.
func EncodePlan(plan *ArtifactGCPlan) ([]byte, error) {
	raw, err := plan.Marshal(plan.CreatedAt.Time)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxAnnotationBytes {
		return nil, fmt.Errorf("%w: serialized Plan is %d bytes, maximum is %d; reduce MaxObjects/MaxBytes", ErrPlanLimitExceeded, len(raw), maxAnnotationBytes)
	}
	return raw, nil
}

// DecodePlan parses a serialized Plan and verifies its frozen identity.
func DecodePlan(raw []byte) (*ArtifactGCPlan, error) {
	plan, err := UnmarshalPlan(raw)
	if err != nil {
		return nil, err
	}
	if err := plan.VerifyIdentity(); err != nil {
		return nil, err
	}
	return plan, nil
}

// MemoryPlanStore is an in-memory PlanStore used by tests and local simulations. It is
// not safe for concurrent use without external synchronization.
type MemoryPlanStore struct {
	plans map[string][]byte
}

// NewMemoryPlanStore constructs an empty MemoryPlanStore.
func NewMemoryPlanStore() *MemoryPlanStore {
	return &MemoryPlanStore{plans: make(map[string][]byte)}
}

// Load implements PlanStore.
func (s *MemoryPlanStore) Load(_ context.Context, key string) (*ArtifactGCPlan, error) {
	raw, found := s.plans[key]
	if !found {
		return nil, nil
	}
	return DecodePlan(raw)
}

// Save implements PlanStore.
func (s *MemoryPlanStore) Save(_ context.Context, key string, plan *ArtifactGCPlan) error {
	raw, err := EncodePlan(plan)
	if err != nil {
		return err
	}
	s.plans[key] = raw
	return nil
}
