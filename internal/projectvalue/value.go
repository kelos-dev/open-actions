package projectvalue

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	// MaxSecretCount and MaxVariableCount match GitHub's per-repository limits.
	MaxSecretCount   = 100
	MaxVariableCount = 500
	// MaxValueBytes matches GitHub's maximum secret size.
	MaxValueBytes = 48 * 1024
)

var namePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

// ValidateName checks the canonical name used in the secrets and vars contexts.
func ValidateName(name string) error {
	if len(name) > 255 || !namePattern.MatchString(name) || strings.HasPrefix(name, "GITHUB_") {
		return fmt.Errorf("must contain at most 255 uppercase letters, digits, or underscores, must not start with a digit, and must not start with GITHUB_")
	}
	return nil
}
