package artifact

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	wfv1alpha1 "github.com/argoproj/argo-workflows/v4/pkg/client/clientset/versioned/typed/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
	"github.com/argoproj/argo-workflows/v4/workflow/artifactgc"
	"github.com/argoproj/argo-workflows/v4/workflow/artifacts"
	artifactlogging "github.com/argoproj/argo-workflows/v4/workflow/artifacts/logging"
	"github.com/argoproj/argo-workflows/v4/workflow/common"
)

// taskPlanStore persists ArtifactGCPlans as an annotation on the
// WorkflowArtifactGCTask CRD. No CRD schema change is required and no credential value
// is ever stored: the Plan only holds non-secret storage coordinates.
type taskPlanStore struct {
	iface wfv1alpha1.WorkflowArtifactGCTaskInterface
}

func (s *taskPlanStore) Load(ctx context.Context, taskName string) (*artifactgc.ArtifactGCPlan, error) {
	task, err := s.iface.Get(ctx, taskName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to load Artifact GC Task %q: %w", taskName, err)
	}
	raw, found := task.Annotations[common.AnnotationKeyArtifactGCPlan]
	if !found || raw == "" {
		return nil, nil
	}
	return artifactgc.DecodePlan([]byte(raw))
}

func (s *taskPlanStore) Save(ctx context.Context, taskName string, plan *artifactgc.ArtifactGCPlan) error {
	raw, err := artifactgc.EncodePlan(plan)
	if err != nil {
		return err
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				common.AnnotationKeyArtifactGCPlan: string(raw),
			},
		},
	})
	if err != nil {
		return err
	}
	_, err = s.iface.Patch(ctx, taskName, types.MergePatchType, patch, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("failed to persist Artifact GC Plan on task %q: %w", taskName, err)
	}
	return nil
}

// specArtifactLocator resolves the runtime (relocated) artifact specifications held by
// a task Spec. Secret references stay on the Spec and are never copied into a Plan.
type specArtifactLocator struct {
	artifacts map[string]*v1alpha1.Artifact
}

func (l *specArtifactLocator) FindArtifact(nodeID, artifactName string) (*v1alpha1.Artifact, bool) {
	a, ok := l.artifacts[nodeID+"/"+artifactName]
	return a, ok
}

// runArtifactGCPlan executes one mode of the versioned Plan/Apply flow for every
// WorkflowArtifactGCTask selected by the Pod hash label. The plan mode performs no
// deletion; the apply mode only addresses versions frozen in the persisted Plan.
func runArtifactGCPlan(ctx context.Context, mode, labelSelector string,
	taskInterface wfv1alpha1.WorkflowArtifactGCTaskInterface, res resources) error {
	log := logging.RequireLoggerFromContext(ctx)
	taskList, err := taskInterface.List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return err
	}
	store := &taskPlanStore{iface: taskInterface}

	for i := range taskList.Items {
		task := &taskList.Items[i]
		switch mode {
		case common.ArtifactGCModePlan:
			log.WithField("task", task.Name).Info(ctx, "building Artifact GC Plan (preview performs no deletion)")
			if err := buildPlanForTask(ctx, task, store, res); err != nil {
				return err
			}
		case common.ArtifactGCModeApply:
			log.WithField("task", task.Name).Info(ctx, "applying frozen Artifact GC Plan")
			if err := applyPlanForTask(ctx, task, store, res); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown %s %q: expected %q or %q", common.EnvVarArtifactGCMode, mode,
				common.ArtifactGCModePlan, common.ArtifactGCModeApply)
		}
	}
	return nil
}

// planInput is one artifact to snapshot, paired with its relocated specification.
type planInput struct {
	nodeID       string
	artifactName string
	artifact     v1alpha1.Artifact
	prefix       bool
}

// collectPlanInputs relocates every artifact in a task Spec and determines whether it
// addresses a single object or a key prefix.
func collectPlanInputs(task *v1alpha1.WorkflowArtifactGCTask) ([]planInput, error) {
	inputs := make([]planInput, 0)
	for nodeName, nodeSpec := range task.Spec.ArtifactsByNode {
		var archiveLocation *v1alpha1.ArtifactLocation
		if nodeSpec.ArchiveLocation != nil {
			archiveLocation = nodeSpec.ArchiveLocation
		}
		for name := range nodeSpec.Artifacts {
			artifact := nodeSpec.Artifacts[name]
			if archiveLocation != nil {
				if err := artifact.Relocate(archiveLocation); err != nil {
					return nil, err
				}
			}
			inputs = append(inputs, planInput{
				nodeID:       nodeName,
				artifactName: name,
				artifact:     artifact,
				prefix:       artifact.S3 != nil && strings.HasSuffix(artifact.S3.Key, "/"),
			})
		}
	}
	return inputs, nil
}

func workflowIdentityFromTask(task *v1alpha1.WorkflowArtifactGCTask) (artifactgc.WorkflowIdentity, error) {
	identity := artifactgc.WorkflowIdentity{
		Namespace:       task.Namespace,
		ResourceVersion: osEnv(common.EnvVarArtifactGCWorkflowRV),
		UID:             osEnv(common.EnvVarArtifactGCWorkflowUID),
	}
	for _, owner := range task.OwnerReferences {
		if owner.Kind == "Workflow" {
			identity.Name = owner.Name
			if identity.UID == "" {
				identity.UID = string(owner.UID)
			}
			break
		}
	}
	if identity.Name == "" || identity.UID == "" {
		return identity, fmt.Errorf("WorkflowArtifactGCTask %q has no resolvable Workflow owner identity", task.Name)
	}
	return identity, nil
}

func osEnv(key string) string { return os.Getenv(key) }

// buildPlanForTask snapshots every artifact through its real driver and freezes the
// finite object/version list. It never deletes.
func buildPlanForTask(ctx context.Context, task *v1alpha1.WorkflowArtifactGCTask,
	store *taskPlanStore, res resources) error {
	inputs, err := collectPlanInputs(task)
	if err != nil {
		return err
	}
	identity, err := workflowIdentityFromTask(task)
	if err != nil {
		return err
	}

	planner := artifactgc.NewPlanner(artifactgc.LimitsFromEnv(), time.Now)
	targets := make([]artifactgc.PlanTarget, 0, len(inputs))
	for i := range inputs {
		in := inputs[i]
		target, planErr := buildOneTarget(ctx, planner, in, res)
		if planErr != nil {
			return planErr
		}
		targets = append(targets, *target)
	}

	plan, err := planner.BuildPlan(identity, taskStrategy(task), targets)
	if err != nil {
		return err
	}
	return store.Save(ctx, task.Name, plan)
}

// buildOneTarget resolves the driver and freezes one artifact's object versions. A
// driver without the versioned capability is recorded as an explicit Unsupported
// target; a bare delete is never substituted.
func buildOneTarget(ctx context.Context, planner *artifactgc.Planner, in planInput, res resources) (*artifactgc.PlanTarget, error) {
	input := artifactgc.PlanInput{
		NodeID:       in.nodeID,
		ArtifactName: in.artifactName,
		Artifact:     &in.artifact,
		Prefix:       in.prefix,
	}
	if in.artifact.S3 == nil {
		return artifactgc.UnsupportedTarget(input,
			artifactgc.ErrVersionedGCUnsupported.Error()+": only S3 artifacts support the versioned Plan flow"), nil
	}
	drv, err := artifacts.NewDriver(ctx, &in.artifact, res)
	if err != nil {
		return nil, err
	}
	versioned, ok := artifactgc.AsVersionedGCDriver(artifactlogging.Unwrap(drv))
	if !ok {
		return artifactgc.UnsupportedTarget(input, artifactgc.ErrVersionedGCUnsupported.Error()), nil
	}
	return planner.BuildTarget(ctx, versioned, input)
}

// applyPlanForTask runs the frozen Plan without re-enumerating any prefix.
func applyPlanForTask(ctx context.Context, task *v1alpha1.WorkflowArtifactGCTask,
	store *taskPlanStore, res resources) error {
	plan, err := store.Load(ctx, task.Name)
	if err != nil {
		return err
	}
	if plan == nil {
		return fmt.Errorf("cannot apply: WorkflowArtifactGCTask %q carries no frozen Artifact GC Plan", task.Name)
	}

	inputs, err := collectPlanInputs(task)
	if err != nil {
		return err
	}
	locator := &specArtifactLocator{artifacts: make(map[string]*v1alpha1.Artifact, len(inputs))}
	for i := range inputs {
		art := inputs[i].artifact
		locator.artifacts[inputs[i].nodeID+"/"+inputs[i].artifactName] = &art
	}

	applier := artifactgc.NewApplier(
		func(driverCtx context.Context, artifact *v1alpha1.Artifact) (artifactgc.VersionedGCDriver, error) {
			drv, drvErr := artifacts.NewDriver(driverCtx, artifact, res)
			if drvErr != nil {
				return nil, drvErr
			}
			versioned, ok := artifactgc.AsVersionedGCDriver(artifactlogging.Unwrap(drv))
			if !ok {
				// Explicit Unsupported: the applier records this and never bare-deletes.
				return nil, nil
			}
			return versioned, nil
		},
		func(_ context.Context, identity artifactgc.WorkflowIdentity) error {
			wantUID := osEnv(common.EnvVarArtifactGCWorkflowUID)
			if wantUID != "" && wantUID != identity.UID {
				return fmt.Errorf("workflow UID mismatch: Pod expects %q but Plan was built for %q", wantUID, identity.UID)
			}
			return nil
		},
		time.Now,
	)

	if applyErr := applier.Apply(ctx, plan, locator); applyErr != nil {
		// Persist whatever per-object results were recorded before returning.
		_ = store.Save(ctx, task.Name, plan)
		return applyErr
	}
	if saveErr := store.Save(ctx, task.Name, plan); saveErr != nil {
		return saveErr
	}

	// A non-complete Plan keeps the GC work open: fail the Pod so the controller
	// requeues and resumes from the persisted per-object results instead of marking
	// the strategy or finalizer successful.
	switch {
	case plan.HasUnsupportedTarget() || plan.HasRefusalResult():
		return fmt.Errorf("Artifact GC Plan %s reached a terminal non-recoverable state", plan.PlanID)
	case plan.HasRetryableObjects():
		return fmt.Errorf("Artifact GC Plan %s still has unconfirmed or pending object versions", plan.PlanID)
	case !plan.PromisesFullCleanup():
		return fmt.Errorf("Artifact GC Plan %s is incomplete and cannot promise full cleanup", plan.PlanID)
	default:
		return nil
	}
}

// taskStrategy returns the GC strategy carried by the task's owning Pod annotation.
// The Plan stores the same strategy string the controller used to create the Pod.
func taskStrategy(task *v1alpha1.WorkflowArtifactGCTask) string {
	if task.Annotations != nil {
		if s := task.Annotations[common.AnnotationKeyArtifactGCStrategy]; s != "" {
			return s
		}
	}
	return ""
}
