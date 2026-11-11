package artifactgc

import (
	"os"
	"strconv"
	"time"
)

// Environment variables shared by the workflow-controller and the Artifact GC Pods
// (argoexec). Every variable is documented in docs/environment-variables.md.
const (
	// EnvVarPlanEnabled gates the Plan/Apply flow. The default (false) keeps the
	// legacy prefix-enumerating GC path unchanged.
	EnvVarPlanEnabled = "ARGO_ARTIFACT_GC_PLAN_ENABLED"
	// EnvVarPlanMaxObjects bounds the number of object versions one Plan may freeze.
	EnvVarPlanMaxObjects = "ARGO_ARTIFACT_GC_PLAN_MAX_OBJECTS"
	// EnvVarPlanMaxBytes bounds the summed size of frozen object versions.
	EnvVarPlanMaxBytes = "ARGO_ARTIFACT_GC_PLAN_MAX_BYTES"
	// EnvVarPlanTTL bounds how long a frozen Plan remains applicable.
	EnvVarPlanTTL = "ARGO_ARTIFACT_GC_PLAN_TTL"
)

// PlanEnabled reports whether the versioned Plan/Apply Artifact GC flow is enabled.
// It defaults to false so existing deployments keep the legacy GC behavior.
func PlanEnabled() bool {
	v, err := strconv.ParseBool(os.Getenv(EnvVarPlanEnabled))
	return err == nil && v
}

// LimitsFromEnv builds PlanLimits from the environment, applying validated defaults.
// Non-positive or malformed values fall back to the defaults rather than failing Pod
// startup.
func LimitsFromEnv() PlanLimits {
	limits := PlanLimits{
		MaxObjects: DefaultMaxObjects,
		MaxBytes:   DefaultMaxBytes,
		TTLSeconds: int64(DefaultTTL / time.Second),
	}
	if raw := os.Getenv(EnvVarPlanMaxObjects); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limits.MaxObjects = parsed
		}
	}
	if raw := os.Getenv(EnvVarPlanMaxBytes); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed > 0 {
			limits.MaxBytes = parsed
		}
	}
	if raw := os.Getenv(EnvVarPlanTTL); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			limits.TTLSeconds = int64(parsed / time.Second)
		}
	}
	return limits
}
