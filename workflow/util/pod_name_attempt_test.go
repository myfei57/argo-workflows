package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNodeAttemptFromName(t *testing.T) {
	tests := map[string]int{
		// Nodes without a retry suffix are the first attempt.
		"node":               0,
		"hello-world":        0,
		// Retry attempt nodes carry a "(n)" suffix.
		"retry-node(0)":      0,
		"retry-node(1)":      1,
		"retry-node(42)":     42,
		"a/b[1].retry(3)":    3,
		// Malformed suffixes degrade to zero rather than failing the pod.
		"retry-node()":       0,
		"retry-node(x)":      0,
		"retry-node(-1)":     0,
		"retry-node(1":       0,
		"()":                 0,
	}
	for name, want := range tests {
		assert.Equal(t, want, NodeAttemptFromName(name), name)
	}
}
