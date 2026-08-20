package runner

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

const (
	needsContextVersion     = 1
	maxNeedsContextBytes    = 900_000
	maxNeeds                = 1000
	maxJobOutputValueLength = 4096
)

type Need struct {
	Result  string            `json:"result"`
	Outputs map[string]string `json:"outputs"`
}

type Needs map[string]Need

type needsContext struct {
	Version int   `json:"version"`
	Needs   Needs `json:"needs"`
}

func (n Needs) ExpressionValues() map[string]any {
	values := make(map[string]any, len(n))
	for jobID, need := range n {
		outputs := make(map[string]any, len(need.Outputs))
		for name, value := range need.Outputs {
			outputs[name] = value
		}
		values[jobID] = map[string]any{
			"result":  need.Result,
			"outputs": outputs,
		}
	}
	return values
}

func EncodeNeedsContext(needs Needs) ([]byte, error) {
	context := needsContext{Version: needsContextVersion, Needs: needs}
	if err := validateNeedsContext(context); err != nil {
		return nil, err
	}
	if needsContextContentBytes(needs) > maxNeedsContextBytes {
		return nil, fmt.Errorf("needs context exceeds %d bytes", maxNeedsContextBytes)
	}
	data, err := json.Marshal(context)
	if err != nil {
		return nil, fmt.Errorf("encode needs context: %w", err)
	}
	if len(data) > maxNeedsContextBytes {
		return nil, fmt.Errorf("needs context exceeds %d bytes", maxNeedsContextBytes)
	}
	return data, nil
}

func DecodeNeedsContext(data []byte) (Needs, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("needs context is empty")
	}
	if len(data) > maxNeedsContextBytes {
		return nil, fmt.Errorf("needs context exceeds %d bytes", maxNeedsContextBytes)
	}
	context := needsContext{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&context); err != nil {
		return nil, fmt.Errorf("decode needs context: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, fmt.Errorf("decode needs context: trailing JSON value")
	}
	if err := validateNeedsContext(context); err != nil {
		return nil, err
	}
	return context.Needs, nil
}

func LoadNeedsContext(plan *Plan, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read needs context: %w", err)
	}
	needs, err := DecodeNeedsContext(data)
	if err != nil {
		return err
	}
	plan.Needs = needs
	return nil
}

func validateNeedsContext(context needsContext) error {
	if context.Version != needsContextVersion {
		return fmt.Errorf("unsupported needs context version %d", context.Version)
	}
	if len(context.Needs) == 0 {
		return fmt.Errorf("needs context has no dependencies")
	}
	if len(context.Needs) > maxNeeds {
		return fmt.Errorf("needs context defines %d dependencies; maximum is %d", len(context.Needs), maxNeeds)
	}
	for jobID, need := range context.Needs {
		if len(jobID) > MaxJobOutputNameLength || !jobOutputNamePattern.MatchString(jobID) {
			return fmt.Errorf("needs context contains invalid job ID %q", jobID)
		}
		switch need.Result {
		case "success", "failure", "skipped", "cancelled":
		default:
			return fmt.Errorf("needs context contains invalid result %q for job %q", need.Result, jobID)
		}
		for name, value := range need.Outputs {
			if len(name) > MaxJobOutputNameLength || !jobOutputNamePattern.MatchString(name) {
				return fmt.Errorf("needs context for job %q contains invalid output name %q", jobID, name)
			}
			if len(value) > maxJobOutputValueLength {
				return fmt.Errorf("needs context output %q for job %q exceeds %d bytes", name, jobID, maxJobOutputValueLength)
			}
		}
	}
	return nil
}

func needsContextContentBytes(needs Needs) int {
	total := 0
	for jobID, need := range needs {
		total += len(jobID) + len(need.Result)
		for name, value := range need.Outputs {
			total += len(name) + len(value)
			if total > maxNeedsContextBytes {
				return total
			}
		}
	}
	return total
}
