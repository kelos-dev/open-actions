package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
)

const (
	// ResultVersion identifies the result format assigned to PlanVersion. A runner
	// that accepts multiple plan versions must emit the result version assigned to
	// the received plan.
	ResultVersion          = 1
	MaxResultBytes         = 4 * 1024
	MaxJobOutputs          = 100
	MaxJobOutputNameLength = 256
)

var jobOutputNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

// Result is the bounded document reported by a completed runner process.
type Result struct {
	Version int               `json:"version"`
	Outputs map[string]string `json:"outputs,omitempty"`
}

// NewResult validates and copies the supplied job outputs into a runner result.
func NewResult(outputs map[string]string) (Result, error) {
	result := Result{Version: ResultVersion}
	if len(outputs) > 0 {
		result.Outputs = make(map[string]string, len(outputs))
		for name, value := range outputs {
			result.Outputs[name] = value
		}
	}
	if _, err := EncodeResult(result); err != nil {
		return Result{Version: ResultVersion}, err
	}
	return result, nil
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
	if result.Version != ResultVersion {
		return fmt.Errorf("unsupported runner result version %d", result.Version)
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
