package joblog

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

// ReadTokenCodec grants read access to one job archive using a dedicated key.
// This key is independent of artifact upload and download credentials.
type ReadTokenCodec struct{ key []byte }

func NewReadTokenCodec(key []byte) (*ReadTokenCodec, error) {
	if len(key) < 32 {
		return nil, errors.New("log read signing key must be at least 32 bytes")
	}
	return &ReadTokenCodec{key: append([]byte(nil), key...)}, nil
}

func (c *ReadTokenCodec) signature(job, expires string) []byte {
	mac := hmac.New(sha256.New, c.key)
	mac.Write([]byte("job-log-read\n" + job + "\n" + expires))
	return mac.Sum(nil)
}

func (c *ReadTokenCodec) Sign(job string, expiresAt time.Time) string {
	expires := strconv.FormatInt(expiresAt.Unix(), 10)
	return expires + ":" + base64.RawURLEncoding.EncodeToString(c.signature(job, expires))
}

func (c *ReadTokenCodec) Validate(job, token string, now time.Time) bool {
	expires, signature, found := strings.Cut(token, ":")
	seconds, err := strconv.ParseInt(expires, 10, 64)
	provided, decodeErr := base64.RawURLEncoding.DecodeString(signature)
	return found && err == nil && decodeErr == nil && now.Unix() < seconds && hmac.Equal(provided, c.signature(job, expires))
}
