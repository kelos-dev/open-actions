package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
)

const (
	// ResultVersion identifies the result format assigned to PlanVersion.
	ResultVersion          = 2
	MaxResultBytes         = 4 * 1024
	MaxJobOutputs          = 100
	MaxJobOutputNameLength = 256
)

type ResultConclusion string

const (
	ResultConclusionSuccess   ResultConclusion = "success"
	ResultConclusionFailure   ResultConclusion = "failure"
	ResultConclusionCancelled ResultConclusion = "cancelled"
	ResultConclusionTimedOut  ResultConclusion = "timed_out"
)

var jobOutputNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

// Result is the bounded document reported by a completed runner process.
type Result struct {
	Version    int               `json:"version"`
	Conclusion ResultConclusion  `json:"conclusion,omitempty"`
	Outputs    map[string]string `json:"outputs,omitempty"`
}

// NewResult validates and copies the supplied conclusion and job outputs.
func NewResult(outputs map[string]string, conclusion ResultConclusion) (Result, error) {
	return newResult(outputs, conclusion, ResultVersion)
}

func newResult(outputs map[string]string, conclusion ResultConclusion, version int) (Result, error) {
	result := Result{Version: version}
	if version >= 2 {
		result.Conclusion = conclusion
	}
	if len(outputs) > 0 {
		result.Outputs = make(map[string]string, len(outputs))
		for name, value := range outputs {
			result.Outputs[name] = value
		}
	}
	if _, err := EncodeResult(result); err != nil {
		fallback := conclusion
		if fallback == ResultConclusionSuccess {
			fallback = ResultConclusionFailure
		}
		return Result{Version: version, Conclusion: resultConclusion(version, fallback)}, err
	}
	return result, nil
}

func failureConclusion(version int) ResultConclusion {
	return resultConclusion(version, ResultConclusionFailure)
}

func resultConclusion(version int, conclusion ResultConclusion) ResultConclusion {
	if version >= 2 {
		return conclusion
	}
	return ""
}

// EncodeResult validates and serializes a runner result.
func EncodeResult(result Result) ([]byte, error) {
	if err := validateResult(result); err != nil {
		return nil, err
	}
	data, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("encode runner result: %w", err)
	}
	if len(data) > MaxResultBytes {
		return nil, fmt.Errorf("runner result exceeds %d bytes", MaxResultBytes)
	}
	return data, nil
}

// DecodeResult parses and validates a serialized runner result.
func DecodeResult(data []byte) (Result, error) {
	if len(data) == 0 {
		return Result{}, fmt.Errorf("runner result is empty")
	}
	if len(data) > MaxResultBytes {
		return Result{}, fmt.Errorf("runner result exceeds %d bytes", MaxResultBytes)
	}
	result := Result{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return Result{}, fmt.Errorf("decode runner result: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Result{}, fmt.Errorf("decode runner result: trailing JSON value")
	}
	if err := validateResult(result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func validateResult(result Result) error {
	if result.Version < 1 || result.Version > ResultVersion {
		return fmt.Errorf("unsupported runner result version %d", result.Version)
	}
	if result.Version >= 2 {
		switch result.Conclusion {
		case ResultConclusionSuccess, ResultConclusionFailure, ResultConclusionCancelled, ResultConclusionTimedOut:
		default:
			return fmt.Errorf("runner result contains invalid conclusion %q", result.Conclusion)
		}
	} else if result.Conclusion != "" {
		return errors.New("runner result version 1 must not contain a conclusion")
	}
	if len(result.Outputs) > MaxJobOutputs {
		return fmt.Errorf("runner result defines %d outputs; maximum is %d", len(result.Outputs), MaxJobOutputs)
	}
	for name := range result.Outputs {
		if len(name) > MaxJobOutputNameLength || !jobOutputNamePattern.MatchString(name) {
			return fmt.Errorf("runner result contains invalid output name %q", name)
		}
	}
	return nil
}
