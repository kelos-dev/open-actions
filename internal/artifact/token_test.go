package artifact

import (
	"strings"
	"testing"
	"time"
)

func TestRuntimeTokenIsScopedSignedAndExpires(t *testing.T) {
	now := time.Unix(1000, 0)
	codec := testTokenCodec(t)
	claims := testClaims("job-uid")
	token, err := codec.NewRuntimeToken(now, time.Hour, claims)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := codec.ParseRuntimeToken(token, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ScopeClaim != "Actions.Results:run-uid:job-uid" || parsed.Scope != claims.Scope {
		t.Fatalf("claims = %#v", parsed)
	}
	if _, err := codec.ParseRuntimeToken(token, now.Add(time.Hour)); err == nil {
		t.Fatal("expired artifact token was accepted")
	}
	other, err := NewTokenCodec([]byte(strings.Repeat("b", MinimumSigningKeySize)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.ParseRuntimeToken(token, now.Add(time.Minute)); err == nil {
		t.Fatal("token signed by another key was accepted")
	}
	parts := strings.Split(token, ".")
	parts[1] = "e30"
	if _, err := codec.ParseRuntimeToken(strings.Join(parts, "."), now.Add(time.Minute)); err == nil {
		t.Fatal("tampered artifact token was accepted")
	}
}

func TestBlobSignatureBindsScopeOperationAndExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	codec := testTokenCodec(t)
	scope := testClaims("job-uid").Scope
	encodedScope, signature, err := codec.SignBlob(scope, 10, "write", now.Add(time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := codec.ValidateBlob(encodedScope, 10, "write", now.Add(time.Hour).Unix(), signature, now)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != scope {
		t.Fatalf("scope = %#v, want %#v", parsed, scope)
	}
	for _, test := range []struct {
		id        int64
		operation string
		expiresAt int64
	}{
		{id: 11, operation: "write", expiresAt: now.Add(time.Hour).Unix()},
		{id: 10, operation: "read", expiresAt: now.Add(time.Hour).Unix()},
		{id: 10, operation: "write", expiresAt: now.Add(time.Hour + time.Second).Unix()},
	} {
		if _, err := codec.ValidateBlob(encodedScope, test.id, test.operation, test.expiresAt, signature, now); err == nil {
			t.Fatalf("signature accepted for id=%d operation=%q expiresAt=%d", test.id, test.operation, test.expiresAt)
		}
	}
	if _, err := codec.ValidateBlob(encodedScope, 10, "write", now.Add(time.Hour).Unix(), signature, now.Add(time.Hour)); err == nil {
		t.Fatal("expired blob signature was accepted")
	}
}

func TestTokenCodecRejectsShortSigningKey(t *testing.T) {
	if _, err := NewTokenCodec([]byte("short")); err == nil {
		t.Fatal("short artifact signing key was accepted")
	}
}

func testTokenCodec(t *testing.T) *TokenCodec {
	t.Helper()
	codec, err := NewTokenCodec([]byte(strings.Repeat("a", MinimumSigningKeySize)))
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func testClaims(jobID string) TokenClaims {
	return TokenClaims{
		Scope:                Scope{ProjectUID: "project-uid", RepositoryID: 123, RootRunUID: "root-uid", RunUID: "run-uid", Attempt: 1},
		WorkflowRunBackendID: "run-uid", WorkflowJobBackendID: jobID,
	}
}
