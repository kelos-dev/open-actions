package expression

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

type hashPattern struct {
	segments []string
	negate   bool
	absolute bool
}

func evaluateHashFiles(arguments []value, evaluation *evaluation) (value, error) {
	github, err := evaluation.context("github")
	if err != nil {
		return value{}, err
	}
	workspaceValue, found, err := github.lookupProperty("workspace")
	if err != nil {
		return value{}, err
	}
	if !found {
		return value{}, fmt.Errorf("github.workspace is unavailable for function \"hashFiles\"")
	}
	workspace, err := workspaceValue.stringValue()
	if err != nil || workspace == "" {
		return value{}, fmt.Errorf("github.workspace is invalid for function \"hashFiles\"")
	}
	patterns, sensitive, err := hashFilePatterns(arguments)
	if err != nil {
		return value{}, err
	}
	digest, err := hashWorkspaceFiles(workspace, patterns)
	if err != nil {
		return value{}, err
	}
	return value{kind: stringKind, text: digest, sensitive: sensitive}, nil
}

func hashFilePatterns(arguments []value) ([]hashPattern, bool, error) {
	var patterns []hashPattern
	sensitive := false
	for _, argument := range arguments {
		input, err := argument.stringValue()
		if err != nil {
			return nil, false, fmt.Errorf("hashFiles patterns must be scalars")
		}
		if len(input) > maxEvaluationStringBytes {
			return nil, false, evaluationSizeError()
		}
		sensitive = sensitive || argument.sensitive
		for _, line := range strings.Split(strings.ReplaceAll(input, "\r\n", "\n"), "\n") {
			line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			negate := false
			for strings.HasPrefix(line, "!") {
				negate = !negate
				line = strings.TrimSpace(strings.TrimPrefix(line, "!"))
			}
			if line == "" {
				return nil, false, fmt.Errorf("hashFiles pattern is empty")
			}
			line = filepath.ToSlash(line)
			absolute := path.IsAbs(line)
			cleaned := path.Clean(line)
			if !absolute && (cleaned == ".." || strings.HasPrefix(cleaned, "../")) {
				return nil, false, fmt.Errorf("hashFiles patterns must remain within github.workspace")
			}
			var segments []string
			if cleaned != "." && cleaned != "/" {
				segments = strings.Split(strings.TrimPrefix(cleaned, "/"), "/")
			}
			for _, segment := range segments {
				if segment == "**" {
					continue
				}
				if _, err := path.Match(segment, ""); err != nil {
					return nil, false, fmt.Errorf("hashFiles pattern is invalid: %w", err)
				}
			}
			patterns = append(patterns, hashPattern{segments: segments, negate: negate, absolute: absolute})
		}
	}
	return patterns, sensitive, nil
}

func hashWorkspaceFiles(workspace string, patterns []hashPattern) (string, error) {
	workspacePath, err := filepath.Abs(workspace)
	if err != nil {
		return "", fmt.Errorf("resolve github.workspace: %w", err)
	}
	root, err := filepath.EvalSymlinks(workspacePath)
	if err != nil {
		return "", fmt.Errorf("resolve github.workspace: %w", err)
	}
	aggregate := sha256.New()
	matched := false
	err = filepath.WalkDir(root, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, filePath)
		if err != nil {
			return err
		}
		workspaceFilePath := filepath.Join(workspacePath, relative)
		if !matchesHashPatterns(filepath.ToSlash(relative), filepath.ToSlash(workspaceFilePath), filepath.ToSlash(filePath), patterns) {
			return nil
		}
		resolved, err := filepath.EvalSymlinks(filePath)
		if err != nil {
			return err
		}
		inside, err := filepath.Rel(root, resolved)
		if err != nil || inside == ".." || strings.HasPrefix(inside, ".."+string(filepath.Separator)) {
			return nil
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		file, err := os.Open(resolved)
		if err != nil {
			return err
		}
		fileDigest := sha256.New()
		_, copyErr := io.Copy(fileDigest, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if _, err := aggregate.Write(fileDigest.Sum(nil)); err != nil {
			return err
		}
		matched = true
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("hash files in github.workspace: %w", err)
	}
	if !matched {
		return "", nil
	}
	return hex.EncodeToString(aggregate.Sum(nil)), nil
}

func matchesHashPatterns(relative, workspaceAbsolute, resolvedAbsolute string, patterns []hashPattern) bool {
	matched := false
	for _, pattern := range patterns {
		candidates := []string{relative}
		if pattern.absolute {
			candidates = []string{
				strings.TrimPrefix(workspaceAbsolute, "/"),
				strings.TrimPrefix(resolvedAbsolute, "/"),
			}
		}
		patternMatched := false
		for _, candidate := range candidates {
			candidateSegments := strings.Split(candidate, "/")
			if matchPathSegments(pattern.segments, candidateSegments) ||
				matchPathSegments(append(append([]string(nil), pattern.segments...), "**"), candidateSegments) {
				patternMatched = true
				break
			}
		}
		if patternMatched {
			matched = !pattern.negate
		}
	}
	return matched
}

func matchPathSegments(pattern, candidate []string) bool {
	type position struct{ pattern, candidate int }
	results := make(map[position]bool)
	visited := make(map[position]bool)
	var match func(int, int) bool
	match = func(patternIndex, candidateIndex int) bool {
		current := position{pattern: patternIndex, candidate: candidateIndex}
		if visited[current] {
			return results[current]
		}
		visited[current] = true
		if patternIndex == len(pattern) {
			results[current] = candidateIndex == len(candidate)
			return results[current]
		}
		if pattern[patternIndex] == "**" {
			results[current] = match(patternIndex+1, candidateIndex) ||
				candidateIndex < len(candidate) && match(patternIndex, candidateIndex+1)
			return results[current]
		}
		if candidateIndex == len(candidate) {
			return false
		}
		segmentMatched, err := path.Match(pattern[patternIndex], candidate[candidateIndex])
		results[current] = err == nil && segmentMatched && match(patternIndex+1, candidateIndex+1)
		return results[current]
	}
	return match(0, 0)
}
