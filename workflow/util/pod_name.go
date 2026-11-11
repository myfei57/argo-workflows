package util

import (
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
	"strings"

	"github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/workflow/common"
)

const (
	maxK8sResourceNameLength = 253
	k8sNamingHashLength      = 10
	maxPrefixLength          = maxK8sResourceNameLength - k8sNamingHashLength
)

// PodNameVersion stores which type of pod names should be used.
// v1 represents the node id.
// v2 is the combination of a node id and template name.
type PodNameVersion string

const (
	// PodNameV1 is the v1 name that uses node ids for pod names
	PodNameV1 PodNameVersion = "v1"
	// PodNameV2 is the v2 name that uses node id combined with
	// the template name
	PodNameV2             PodNameVersion = "v2"
	DefaultPodNameVersion PodNameVersion = PodNameV2
)

// String stringifies the pod name version
func (v PodNameVersion) String() string {
	return string(v)
}

// GetPodNameVersion returns the pod name version to be used
func GetPodNameVersion() PodNameVersion {
	switch os.Getenv("POD_NAMES") {
	case "v2":
		return PodNameV2
	case "v1":
		return PodNameV1
	default:
		return DefaultPodNameVersion
	}
}

// GeneratePodName return a deterministic pod name
func GeneratePodName(workflowName, nodeName, templateName, nodeID string, version PodNameVersion) string {
	if version == PodNameV1 {
		return nodeID
	}

	if workflowName == nodeName {
		return workflowName
	}

	prefix := workflowName
	if templateName != "" {
		prefix = fmt.Sprintf("%s-%s", prefix, templateName)
	}
	prefix = ensurePodNamePrefixLength(prefix)

	// The node ID is the workflow name followed by the hash of the node name
	// (with any collision suffix), so take the hash from there rather than
	// rehashing nodeName, which would put colliding nodes in the same pod.
	if hash, ok := strings.CutPrefix(nodeID, workflowName+"-"); ok {
		// a widened 64-bit node ID can exceed the k8sNamingHashLength budget
		// ensurePodNamePrefixLength reserves; shorten the prefix, not the hash
		if excess := len(prefix) + 1 + len(hash) - maxK8sResourceNameLength; excess > 0 {
			prefix = prefix[:len(prefix)-excess]
		}
		return fmt.Sprintf("%s-%s", prefix, hash)
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(nodeName))
	return fmt.Sprintf("%s-%v", prefix, h.Sum32())
}

func ensurePodNamePrefixLength(prefix string) string {
	if len(prefix) > maxPrefixLength-1 {
		return prefix[0 : maxPrefixLength-1]
	}

	return prefix
}

// GetWorkflowPodNameVersion gets the pod name version from the annotation of a
// given workflow
func GetWorkflowPodNameVersion(wf *v1alpha1.Workflow) PodNameVersion {
	annotations := wf.GetAnnotations()
	version, ok := annotations[common.AnnotationKeyPodNameVersion]
	if !ok {
		return GetPodNameVersion()
	}
	switch version {
	case PodNameV1.String():
		return PodNameV1
	case PodNameV2.String():
		return PodNameV2
	default:
		return DefaultPodNameVersion
	}
}

// NodeAttemptFromName returns the zero-based retry attempt encoded in a
// node name. Retry attempt nodes are named "<retry-node>(<n>)" (see
// handleRetries); a node without the suffix is the first attempt and
// returns 0. A malformed suffix also returns 0 rather than an error so
// the executor never fails a pod over an observability field.
func NodeAttemptFromName(nodeName string) int {
	open := strings.LastIndex(nodeName, "(")
	if open <= 0 || !strings.HasSuffix(nodeName, ")") {
		return 0
	}
	attempt, err := strconv.Atoi(nodeName[open+1 : len(nodeName)-1])
	if err != nil || attempt < 0 {
		return 0
	}
	return attempt
}
