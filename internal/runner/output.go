package runner

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	maxMaskValueBytes    = 384 << 10
	maxOutputBufferBytes = 512 << 10
	minDerivedMaskBytes  = 6
)

type outputMasker struct {
	mutex sync.RWMutex
	masks [][]byte
}

type maskingWriter struct {
	masker *outputMasker
	target io.Writer
	mutex  sync.Mutex
	buffer []byte
}

func newOutputMasker(values ...string) *outputMasker {
	masker := &outputMasker{}
	for _, value := range values {
		masker.add(value)
	}
	return masker
}

func (m *outputMasker) writer(target io.Writer) *maskingWriter {
	return &maskingWriter{masker: m, target: target}
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

func (m *outputMasker) addSecret(value string) {
	if value == "" {
		return
	}
	m.addSecretValue(value)
	for _, line := range strings.FieldsFunc(value, func(character rune) bool { return character == '\r' || character == '\n' }) {
		line = strings.TrimSpace(line)
		if len(line) >= minDerivedMaskBytes && line != value {
			m.addSecretValue(line)
		}
	}
}

func (m *outputMasker) addSecretValue(value string) {
	m.add(value)
	valueBytes := []byte(value)
	for shift := 0; shift < 3 && shift < len(valueBytes); shift++ {
		m.add(base64.StdEncoding.EncodeToString(valueBytes[shift:]))
		m.add(base64.RawStdEncoding.EncodeToString(valueBytes[shift:]))
	}
	encodedJSON, err := json.Marshal(value)
	if err == nil && len(encodedJSON) >= 2 {
		m.add(string(encodedJSON[1 : len(encodedJSON)-1]))
	}
	m.add(strings.ReplaceAll(url.QueryEscape(value), "+", "%20"))
	m.add(strings.ReplaceAll(value, `"`, `\"`))
	m.add(strings.ReplaceAll(value, `'`, `''`))
	m.add(strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	).Replace(value))
	if len(value) > 8 && strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		m.add(value[1 : len(value)-1])
	}
	if separator := strings.LastIndex(value, "&"); separator >= 0 {
		m.addSecretSection(value[:separator+1])
		m.addSecretSection(value[separator+1:])
	}
	if separator := strings.Index(value, "&+"); separator >= 0 {
		m.addSecretSection(value[:separator+2])
		remainder := value[separator+2:]
		_, size := utf8.DecodeRuneInString(remainder)
		if size < len(remainder) {
			m.addSecretSection(remainder[size:])
		}
	}
}

func (m *outputMasker) addSecretSection(value string) {
	if len(value) >= minDerivedMaskBytes {
		m.add(value)
	}
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

func (m *outputMasker) contains(value string) bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, secret := range m.masks {
		if strings.Contains(value, string(secret)) {
			return true
		}
	}
	return false
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
	masked, consumed := w.masker.maskPrefix(w.buffer, final)
	if consumed == 0 {
		return nil
	}
	if err := writeAll(w.target, masked); err != nil {
		return err
	}
	w.buffer = w.buffer[consumed:]
	return nil
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
