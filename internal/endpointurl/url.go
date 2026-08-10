package endpointurl

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

// Normalize validates and canonicalizes an HTTP endpoint URL.
func Normalize(value, label string, trailingSlash bool) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", label, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("%s must use http or https", label)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("%s must include a host", label)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("%s must not include user information", label)
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", fmt.Errorf("%s must not include a query", label)
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("%s must not include a fragment", label)
	}
	if parsed.Opaque != "" || parsed.RawPath != "" {
		return "", fmt.Errorf("%s must use an unescaped hierarchical path", label)
	}
	if strings.Contains(parsed.Path, "//") {
		return "", fmt.Errorf("%s path must not contain empty segments", label)
	}
	trimmedPath := strings.TrimRight(parsed.Path, "/")
	if trimmedPath == "" {
		trimmedPath = "/"
	}
	if parsed.Path != "" && path.Clean(parsed.Path) != trimmedPath {
		return "", fmt.Errorf("%s path must not contain empty, '.' or '..' segments", label)
	}
	for _, character := range strings.Trim(parsed.Path, "/") {
		if !isPathCharacter(character) {
			return "", fmt.Errorf("%s path must contain only URL-safe path characters", label)
		}
	}
	if trailingSlash {
		if trimmedPath == "/" {
			parsed.Path = "/"
		} else {
			parsed.Path = trimmedPath + "/"
		}
	} else if trimmedPath == "/" {
		parsed.Path = ""
	} else {
		parsed.Path = trimmedPath
	}
	return parsed.String(), nil
}

// NormalizeOrigin validates an HTTP endpoint URL without a path.
func NormalizeOrigin(value, label string) (string, error) {
	normalized, err := Normalize(value, label, false)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		return "", err
	}
	if parsed.Path != "" {
		return "", fmt.Errorf("%s must not include a path", label)
	}
	return normalized, nil
}

func isPathCharacter(character rune) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' ||
		strings.ContainsRune("-._~/", character)
}
