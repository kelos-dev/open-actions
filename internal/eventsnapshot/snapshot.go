package eventsnapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	Annotation = "actions.kelos.dev/github-event-snapshot"
	DataKey    = "event.json"
	MaxBytes   = 900_000
)

func Decode(data []byte) (map[string]any, error) {
	if len(data) > MaxBytes {
		return nil, fmt.Errorf("GitHub event snapshot exceeds %d bytes", MaxBytes)
	}
	document := map[string]any{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode GitHub event snapshot: %w", err)
	}
	if document == nil {
		return nil, errors.New("decode GitHub event snapshot: document must be an object")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("decode GitHub event snapshot: trailing JSON value")
	}
	return document, nil
}
