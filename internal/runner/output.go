package runner

import (
	"bytes"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
)

const (
	maxMaskValueBytes    = 8 << 10
	maxOutputBufferBytes = 64 << 10
)

var addMaskPrefix = []byte("::add-mask::")

type outputMasker struct {
	mutex sync.RWMutex
	masks [][]byte
}

type maskingWriter struct {
	masker             *outputMasker
	target             io.Writer
	mutex              sync.Mutex
	buffer             []byte
	lineStart          bool
	discardMaskCommand bool
}

func newOutputMasker(values ...string) *outputMasker {
	masker := &outputMasker{}
	for _, value := range values {
		masker.add(value)
	}
	return masker
}

func (m *outputMasker) writer(target io.Writer) *maskingWriter {
	return &maskingWriter{masker: m, target: target, lineStart: true}
}

func (m *outputMasker) add(value string) {
	if value == "" || len(value) > maxMaskValueBytes {
		return
	}
	valueBytes := []byte(value)
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for _, existing := range m.masks {
		if bytes.Equal(existing, valueBytes) {
			return
		}
	}
	m.masks = append(m.masks, valueBytes)
	sort.Slice(m.masks, func(left, right int) bool { return len(m.masks[left]) > len(m.masks[right]) })
}

func (m *outputMasker) maskPrefix(data []byte, final bool) ([]byte, int) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	limit := len(data)
	if !final && len(m.masks) > 0 {
		limit = len(data) - len(m.masks[0]) + 1
		if limit < 1 {
			return nil, 0
		}
	}
	masked := make([]byte, 0, limit)
	position := 0
	for position < limit {
		var matched []byte
		for _, secret := range m.masks {
			if len(data)-position >= len(secret) && bytes.Equal(data[position:position+len(secret)], secret) {
				matched = secret
				break
			}
		}
		if len(matched) > 0 {
			masked = append(masked, "***"...)
			position += len(matched)
			continue
		}
		masked = append(masked, data[position])
		position++
	}
	return masked, position
}

func (m *outputMasker) mask(value string) string {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, secret := range m.masks {
		value = strings.ReplaceAll(value, string(secret), "***")
	}
	return value
}

func (w *maskingWriter) Write(data []byte) (int, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	written := 0
	for len(data) > 0 {
		space := maxOutputBufferBytes - len(w.buffer)
		if space == 0 {
			if err := w.process(false); err != nil {
				return written, err
			}
			space = maxOutputBufferBytes - len(w.buffer)
			if space == 0 {
				return written, errors.New("output masking buffer is full")
			}
		}
		if space > len(data) {
			space = len(data)
		}
		w.buffer = append(w.buffer, data[:space]...)
		data = data[space:]
		written += space
		if err := w.process(false); err != nil {
			return written, err
		}
	}
	return written, nil
}

func (w *maskingWriter) flush() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.process(true)
}

func (w *maskingWriter) process(final bool) error {
	for len(w.buffer) > 0 {
		if w.discardMaskCommand {
			newline := bytes.IndexByte(w.buffer, '\n')
			if newline < 0 {
				w.buffer = w.buffer[:0]
				return nil
			}
			w.buffer = w.buffer[newline+1:]
			w.discardMaskCommand = false
			w.lineStart = true
			continue
		}
		if w.lineStart && possibleAddMaskCommand(w.buffer) {
			completeCommand := bytes.HasPrefix(w.buffer, addMaskPrefix)
			newline := bytes.IndexByte(w.buffer, '\n')
			if newline >= 0 {
				if completeCommand {
					w.addMaskCommand(w.buffer[:newline])
				} else if err := w.writeMasked(w.buffer[:newline+1], true); err != nil {
					return err
				}
				w.buffer = w.buffer[newline+1:]
				w.lineStart = true
				continue
			}
			if final {
				if completeCommand {
					w.addMaskCommand(w.buffer)
				} else if err := w.writeMasked(w.buffer, true); err != nil {
					return err
				}
				w.buffer = nil
				return nil
			}
			if len(w.buffer) > len(addMaskPrefix)+maxMaskValueBytes {
				w.buffer = w.buffer[:0]
				w.discardMaskCommand = true
			}
			return nil
		}
		newline := bytes.IndexByte(w.buffer, '\n')
		if newline >= 0 {
			if err := w.writeMasked(w.buffer[:newline+1], true); err != nil {
				return err
			}
			w.buffer = w.buffer[newline+1:]
			w.lineStart = true
			continue
		}
		if final {
			if err := w.writeMasked(w.buffer, true); err != nil {
				return err
			}
			w.buffer = nil
			return nil
		}
		masked, consumed := w.masker.maskPrefix(w.buffer, false)
		if consumed == 0 {
			return nil
		}
		if err := writeAll(w.target, masked); err != nil {
			return err
		}
		w.buffer = w.buffer[consumed:]
		w.lineStart = false
	}
	return nil
}

func (w *maskingWriter) writeMasked(data []byte, final bool) error {
	masked, consumed := w.masker.maskPrefix(data, final)
	if consumed != len(data) {
		return errors.New("output masker retained a completed line")
	}
	return writeAll(w.target, masked)
}

func (w *maskingWriter) addMaskCommand(command []byte) {
	value := bytes.TrimSuffix(command, []byte{'\r'})
	value = bytes.TrimPrefix(value, addMaskPrefix)
	unescaped := unescapeCommandValue(string(value))
	w.masker.add(unescaped)
	for _, line := range strings.FieldsFunc(unescaped, func(character rune) bool {
		return character == '\r' || character == '\n'
	}) {
		w.masker.add(line)
	}
}

func possibleAddMaskCommand(data []byte) bool {
	if len(data) <= len(addMaskPrefix) {
		return bytes.HasPrefix(addMaskPrefix, data)
	}
	return bytes.HasPrefix(data, addMaskPrefix)
}

func writeAll(target io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := target.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func unescapeCommandValue(value string) string {
	value = strings.ReplaceAll(value, "%0D", "\r")
	value = strings.ReplaceAll(value, "%0A", "\n")
	return strings.ReplaceAll(value, "%25", "%")
}
