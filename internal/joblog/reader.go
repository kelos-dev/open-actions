package joblog

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"time"
)

// WithoutTimestamps removes log timestamps without buffering whole lines.
func WithoutTimestamps(source io.Reader) io.Reader {
	return &outputReader{source: bufio.NewReader(source), lineStart: true}
}

type outputReader struct {
	source    *bufio.Reader
	pending   []byte
	err       error
	lineStart bool
}

func (r *outputReader) Read(output []byte) (int, error) {
	if len(output) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 && r.err == nil {
		data, err := r.source.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			err = nil
		}
		r.err = err
		lineStart := r.lineStart
		r.lineStart = len(data) > 0 && data[len(data)-1] == '\n'
		if lineStart {
			if prefix, content, found := bytes.Cut(data, []byte(" ")); found {
				if _, err := time.Parse(time.RFC3339Nano, string(prefix)); err == nil {
					data = content
				}
			}
		}
		r.pending = data
	}
	n := copy(output, r.pending)
	r.pending = r.pending[n:]
	if len(r.pending) == 0 {
		return n, r.err
	}
	return n, nil
}
