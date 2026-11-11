package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
	"github.com/argoproj/argo-workflows/v4/workflow/artifactgc"
	"github.com/argoproj/argo-workflows/v4/workflow/common"
	"github.com/argoproj/argo-workflows/v4/workflow/controller/indexes"
	"github.com/argoproj/argo-workflows/v4/workflow/util"
)

// planApplyPodSuffix distinguishes the conditional-deletion Pod from the preview Pod
// for the same permission grouping.
const planApplyPodSuffix = "-apply"

// planPhase is the restart-safe state of one Plan-mode permission group. It is derived
// purely from the observed Pods and the persisted Plan annotation, so a controller
// restart resumes exactly where the previous reconcile stopped.
type planPhase string

const (
	planPhaseNeedPlanPod     planPhase = "NeedPlanPod"
	planPhasePlanPodRunning  planPhase = "PlanPodRunning"
	planPhasePlanPodFailed   planPhase = "PlanPodFailed"
	planPhaseNeedApplyPod    planPhase = "NeedApplyPod"
	planPhaseApplyPodRunning planPhase = "ApplyPodRunning"
	planPhaseApplyPodFailed  planPhase = "ApplyPodFailed"
	planPhaseSucceeded       planPhase = "Succeeded"
)

// planModeEligible reports whether a strategy run can use the versioned Plan/Apply
// flow. The first iteration only supports S3 artifacts; a mixed group keeps the legacy
// path so default behavior is unchanged.
func planModeEligible(results wfv1.ArtifactSearchResults) bool {
	if len(results) == 0 {
		return false
	}
	for _, result := range results {
		if result.Artifact.S3 == nil {
			return false
		}
	}
	return true
}

// planGCEnv builds the phase-specific environment injected into a Plan-mode GC Pod.
// The Workflow UID and resourceVersion are frozen here and re-verified at Apply time.
func planGCEnv(wf *wfv1.Workflow, mode string) []corev1.EnvVar {
	return []corev1.EnvVar{
		{Name: common.EnvVarArtifactGCMode, Value: mode},
		{Name: common.EnvVarArtifactGCWorkflowUID, Value: string(wf.UID)},
		{Name: common.EnvVarArtifactGCWorkflowRV, Value: wf.ResourceVersion},
	}
}

// decidePlanPhase is the pure state machine used by both reconciliation and unit
// tests. A missing Plan while the preview Pod succeeded means the Pod could not freeze
// a valid Plan, which is a retried failure rather than success.
func decidePlanPhase(planPod, applyPod *corev1.Pod, plans []*artifactgc.ArtifactGCPlan) planPhase {
	if planPod == nil {
		return planPhaseNeedPlanPod
	}
	switch planPod.Status.Phase {
	case corev1.PodPending, corev1.PodRunning, corev1.PodUnknown:
		return planPhasePlanPodRunning
	case corev1.PodFailed:
		return planPhasePlanPodFailed
	case corev1.PodSucceeded:
		// fall through to Apply-side decisions
	default:
		return planPhasePlanPodRunning
	}

	if len(plans) == 0 {
		// The preview Pod finished without a persisted Plan: nothing may be applied.
		return planPhasePlanPodFailed
	}
	if applyPod == nil {
		return planPhaseNeedApplyPod
	}
	switch applyPod.Status.Phase {
	case corev1.PodPending, corev1.PodRunning, corev1.PodUnknown:
		return planPhaseApplyPodRunning
	case corev1.PodFailed:
		return planPhaseApplyPodFailed
	case corev1.PodSucceeded:
		return planPhaseSucceeded
	default:
		return planPhaseApplyPodRunning
	}
}

// processArtifactGCStrategyPlan runs the two-phase Plan/Apply flow for one strategy.
// The strategy is only marked processed once every permission group has a Plan that
// promises full cleanup; any unsupported, conflicting, incomplete or unconfirmed state
// leaves it unprocessed so the existing retry/abandon window governs recovery.
func (woc *wfOperationCtx) processArtifactGCStrategyPlan(ctx context.Context, strategy wfv1.ArtifactGCStrategy,
	artifactSearchResults wfv1.ArtifactSearchResults) error {
	groupedByPod, podNames, templatesByName, err := woc.groupPlanArtifacts(strategy, artifactSearchResults)
	if err != nil {
		return err
	}

	for podName, templatesToArtList := range groupedByPod {
		tasks := make([]*wfv1.WorkflowArtifactGCTask, 0)
		for templateName, artifacts := range templatesToArtList {
			template := templatesByName[templateName]
			woc.addTemplateArtifactsToTasks(ctx, podName, &tasks, template, artifacts)
		}
		if len(tasks) == 0 {
			continue
		}
		// The executor reads the strategy back from the task when assembling the Plan.
		for i := range tasks {
			if tasks[i].Annotations == nil {
				tasks[i].Annotations = make(map[string]string)
			}
			tasks[i].Annotations[common.AnnotationKeyArtifactGCStrategy] = string(strategy)
			created, createErr := woc.createWorkflowArtifactGCTask(ctx, tasks[i])
			if createErr != nil {
				return createErr
			}
			tasks[i] = created
		}

		phase, plans, err := woc.evaluatePlanGroup(ctx, podName, tasks)
		if err != nil {
			return err
		}
		woc.log.WithFields(logging.Fields{"pod": podName, "phase": phase}).Debug(ctx, "artifact GC Plan group state")

		switch phase {
		case planPhaseNeedPlanPod:
			if err := woc.createPlanPhasePod(ctx, strategy, tasks, podNames[podName], podName,
				templatesToArtList, templatesByName, common.ArtifactGCModePlan, nil); err != nil {
				return err
			}
		case planPhasePlanPodFailed:
			if err := woc.deleteGCPod(ctx, podName); err != nil {
				return err
			}
			return woc.planRetryError(ctx, strategy, "preview Pod failed; it will be rebuilt from the frozen workflow state")
		case planPhaseNeedApplyPod:
			if plansContainRefusal(plans) {
				woc.addArtGCCondition(fmt.Sprintf("Artifact GC Plan for strategy %s refused: storage is unsupported or a frozen version conflicts; no bare delete is attempted", strategy))
				return woc.planRetryError(ctx, strategy, "Plan contains unsupported or conflicting targets")
			}
			if err := woc.createPlanPhasePod(ctx, strategy, tasks, podNames[podName], podName+planApplyPodSuffix,
				templatesToArtList, templatesByName, common.ArtifactGCModeApply, nil); err != nil {
				return err
			}
		case planPhaseApplyPodFailed:
			if err := woc.deleteGCPod(ctx, podName+planApplyPodSuffix); err != nil {
				return err
			}
			// Same-version conditional deletes are idempotent, so a fresh Apply Pod
			// safely resumes: prior successes read back as AlreadyGone.
			return woc.planRetryError(ctx, strategy, "apply Pod failed; it will be retried against the same frozen Plan")
		case planPhaseSucceeded:
			if err := woc.finalizePlanGroup(ctx, strategy, tasks, podName, plans); err != nil {
				return err
			}
			// Advance the processed state only after the frozen Plan has promised full
			// cleanup and the task objects are deleted, so a later reconcile cannot
			// rebuild preview Pods for this strategy.
			woc.markArtifactGCStrategyProcessed(strategy)
		case planPhasePlanPodRunning, planPhaseApplyPodRunning:
			// Wait for the Pod to finish; no strategy/finalizer state advances.
		}
	}
	return nil
}

// groupPlanArtifacts groups search results by GC Pod permissions, mirroring the legacy
// grouping so Service Account and Pod metadata semantics are preserved.
func (woc *wfOperationCtx) groupPlanArtifacts(strategy wfv1.ArtifactGCStrategy,
	artifactSearchResults wfv1.ArtifactSearchResults) (map[string]templatesToArtifacts, map[string]podInfo, map[string]*wfv1.Template, error) {
	groupedByPod := make(map[string]templatesToArtifacts)
	podNames := make(map[string]podInfo)
	templatesByName := make(map[string]*wfv1.Template)

	for _, artifactSearchResult := range artifactSearchResults {
		info := woc.getArtifactGCPodInfo(&artifactSearchResult.Artifact)
		podName, err := woc.artGCPodName(strategy, info)
		if err != nil {
			return nil, nil, nil, err
		}
		if _, found := podNames[podName]; !found {
			podNames[podName] = info
		}
		node, nodeErr := woc.wf.Status.Nodes.Get(artifactSearchResult.NodeID)
		if nodeErr != nil {
			return nil, nil, nil, fmt.Errorf("can't process Artifact GC Strategy %s: node ID %q not found in Status", strategy, artifactSearchResult.NodeID)
		}
		templateName := util.GetTemplateFromNode(*node)
		if templateName == "" {
			return nil, nil, nil, fmt.Errorf("can't process Artifact GC Strategy %s: node %+v has an unnamed template", strategy, node)
		}
		template, found := templatesByName[templateName]
		if !found {
			template = woc.wf.GetTemplateByName(templateName)
			if template == nil {
				return nil, nil, nil, fmt.Errorf("can't process Artifact GC Strategy %s: template name %q belonging to node %+v not found", strategy, templateName, node)
			}
			templatesByName[templateName] = template
		}
		if _, found := groupedByPod[podName]; !found {
			groupedByPod[podName] = make(templatesToArtifacts)
		}
		if _, found := groupedByPod[podName][template.Name]; !found {
			groupedByPod[podName][template.Name] = make(wfv1.ArtifactSearchResults, 0)
		}
		groupedByPod[podName][template.Name] = append(groupedByPod[podName][template.Name], artifactSearchResult)
	}
	return groupedByPod, podNames, templatesByName, nil
}

// evaluatePlanGroup loads the observed Pods and persisted Plans for one permission
// group and computes the restart-safe phase.
func (woc *wfOperationCtx) evaluatePlanGroup(ctx context.Context, podName string,
	tasks []*wfv1.WorkflowArtifactGCTask) (planPhase, []*artifactgc.ArtifactGCPlan, error) {
	planPod, err := woc.getArtifactGCPodByName(podName)
	if err != nil {
		return "", nil, err
	}
	var applyPod *corev1.Pod
	if planPod != nil && planPod.Status.Phase == corev1.PodSucceeded {
		applyPod, err = woc.getArtifactGCPodByName(podName + planApplyPodSuffix)
		if err != nil {
			return "", nil, err
		}
	}
	plans, err := loadPlanFromTasks(ctx, tasks)
	if err != nil {
		return "", nil, err
	}
	return decidePlanPhase(planPod, applyPod, plans), plans, nil
}

// createPlanPhasePod creates one preview or apply Pod with the phase identity env and
// annotation. extraEnv is reserved for callers that need further injection.
func (woc *wfOperationCtx) createPlanPhasePod(ctx context.Context, strategy wfv1.ArtifactGCStrategy,
	tasks []*wfv1.WorkflowArtifactGCTask, info podInfo, podName string,
	templatesToArtList templatesToArtifacts, templatesByName map[string]*wfv1.Template,
	mode string, extraEnv []corev1.EnvVar) error {
	env := append(planGCEnv(woc.wf, mode), extraEnv...)
	annotations := map[string]string{common.AnnotationKeyArtifactGCMode: mode}
	_, err := woc.createArtifactGCPod(ctx, strategy, tasks, info, podName, templatesToArtList,
		templatesByName, env, annotations)
	if err != nil {
		return err
	}
	woc.wf.Status.ArtifactGCStatus.RecordArtifactGCPod(podName)
	woc.updated = true
	return nil
}

// plansContainRefusal reports whether any frozen Plan hit a terminal unsupported or
// version-conflict state that forbids Apply.
func plansContainRefusal(plans []*artifactgc.ArtifactGCPlan) bool {
	for _, plan := range plans {
		if plan.HasUnsupportedTarget() || plan.HasRefusalResult() {
			return true
		}
	}
	return false
}

// finalizePlanGroup marks workflow artifacts Deleted only when every persisted Plan
// promises full cleanup, then removes the task objects so the owned Pods are garbage
// collected. An incomplete or unconfirmed Plan never reaches this function.
func (woc *wfOperationCtx) finalizePlanGroup(ctx context.Context, strategy wfv1.ArtifactGCStrategy,
	tasks []*wfv1.WorkflowArtifactGCTask, podName string, plans []*artifactgc.ArtifactGCPlan) error {
	for _, plan := range plans {
		if !plan.PromisesFullCleanup() {
			woc.addArtGCCondition(fmt.Sprintf("Artifact GC Plan for strategy %s cannot promise full cleanup; finalizer is retained", strategy))
			return woc.planRetryError(ctx, strategy, "Plan is incomplete or has unconfirmed objects")
		}
		for i := range plan.Targets {
			target := &plan.Targets[i]
			wfNode, err := woc.wf.Status.Nodes.Get(target.NodeID)
			if err != nil {
				return fmt.Errorf("node %q from Artifact GC Plan not found in Workflow Status: %w", target.NodeID, err)
			}
			if wfNode.Outputs == nil {
				continue
			}
			for j, wfArtifact := range wfNode.Outputs.Artifacts {
				if wfArtifact.Name != target.ArtifactName {
					continue
				}
				wfNode.Outputs.Artifacts[j].Deleted = true
				woc.wf.Status.Nodes.Set(ctx, target.NodeID, *wfNode)
			}
		}
	}

	for _, task := range tasks {
		err := woc.controller.wfclientset.ArgoprojV1alpha1().WorkflowArtifactGCTasks(woc.wf.Namespace).
			Delete(ctx, task.Name, metav1.DeleteOptions{})
		if err != nil && !apierr.IsNotFound(err) {
			woc.log.WithField("name", task.Name).WithError(err).Error(ctx, "error deleting completed WorkflowArtifactGCTask")
		}
	}
	woc.wf.Status.ArtifactGCStatus.SetArtifactGCPodRecouped(podName, true)
	woc.wf.Status.ArtifactGCStatus.SetArtifactGCPodRecouped(podName+planApplyPodSuffix, true)
	woc.updated = true
	return nil
}

// planModeWorkInProgress reports whether Plan-mode Pods still exist for this Workflow.
// The finalizer must not be removed while this is true.
func (woc *wfOperationCtx) planModeWorkInProgress(ctx context.Context) bool {
	pods, err := woc.controller.PodController.GetPodsByIndex(indexes.WorkflowIndex, woc.wf.GetNamespace()+"/"+woc.wf.GetName())
	if err != nil {
		woc.log.WithError(err).Error(ctx, "failed to list pods while checking Artifact GC Plan progress")
		return true // fail closed: keep the finalizer on uncertainty
	}
	for _, obj := range pods {
		pod := obj.(*corev1.Pod)
		if pod.Labels[common.LabelKeyComponent] != artifactGCComponent {
			continue
		}
		if _, isPlanPod := pod.Annotations[common.AnnotationKeyArtifactGCMode]; isPlanPod {
			return true
		}
	}
	return false
}

// planRetryError records the failure through the existing start-failure machinery so
// the strategy stays unprocessed and the retry/abandon window applies uniformly.
func (woc *wfOperationCtx) planRetryError(ctx context.Context, strategy wfv1.ArtifactGCStrategy, msg string) error {
	return fmt.Errorf("%s: %s", artifactGCStartFailurePrefix(strategy), msg)
}

func (woc *wfOperationCtx) deleteGCPod(ctx context.Context, podName string) error {
	err := woc.controller.kubeclientset.CoreV1().Pods(woc.wf.Namespace).Delete(ctx, podName, metav1.DeleteOptions{})
	if err != nil && !apierr.IsNotFound(err) {
		return fmt.Errorf("failed to delete Artifact GC Pod %q for retry: %w", podName, err)
	}
	return nil
}

func (woc *wfOperationCtx) getArtifactGCPodByName(podName string) (*corev1.Pod, error) {
	pods, err := woc.controller.PodController.GetPodsByIndex(indexes.WorkflowIndex, woc.wf.GetNamespace()+"/"+woc.wf.GetName())
	if err != nil {
		return nil, fmt.Errorf("failed to get pods from informer: %w", err)
	}
	for _, obj := range pods {
		pod := obj.(*corev1.Pod)
		if pod.Name == podName {
			return pod, nil
		}
	}
	return nil, nil
}

func (woc *wfOperationCtx) tasksForPod(ctx context.Context, podName string) ([]*wfv1.WorkflowArtifactGCTask, error) {
	labelSelector := fmt.Sprintf("%s = %s", common.LabelKeyArtifactGCPodHash, woc.artifactGCPodLabel(podName))
	taskList, err := woc.controller.wfclientset.ArgoprojV1alpha1().WorkflowArtifactGCTasks(woc.wf.Namespace).
		List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
	if err != nil {
		return nil, err
	}
	tasks := make([]*wfv1.WorkflowArtifactGCTask, 0, len(taskList.Items))
	for i := range taskList.Items {
		tasks = append(tasks, &taskList.Items[i])
	}
	return tasks, nil
}

// loadPlanFromTasks reads and verifies every persisted Plan of a permission group.
// Each WorkflowArtifactGCTask carries its own Plan (distinct frozen targets, same
// Workflow identity). A missing annotation simply yields no entry so the caller treats
// it as a phase transition rather than a hard error.
func loadPlanFromTasks(_ context.Context, tasks []*wfv1.WorkflowArtifactGCTask) ([]*artifactgc.ArtifactGCPlan, error) {
	plans := make([]*artifactgc.ArtifactGCPlan, 0, len(tasks))
	var workflowUID string
	for _, task := range tasks {
		raw, found := task.Annotations[common.AnnotationKeyArtifactGCPlan]
		if !found || raw == "" {
			continue
		}
		plan, err := artifactgc.DecodePlan([]byte(raw))
		if err != nil {
			return nil, fmt.Errorf("failed to decode Artifact GC Plan on task %q: %w", task.Name, err)
		}
		if workflowUID == "" {
			workflowUID = plan.Workflow.UID
		} else if workflowUID != plan.Workflow.UID {
			return nil, fmt.Errorf("conflicting Workflow identities among Artifact GC Plans of the same Pod group")
		}
		plans = append(plans, plan)
	}
	return plans, nil
}
