package joblog

import (
	"bufio"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/kelos-dev/open-actions/internal/endpointurl"
)

const Path = "/job-logs/"
const IncompleteWarning = "::warning::Archived runner logs may be incomplete because collection did not observe a complete log stream\n"

type Archive interface {
	Open(context.Context, string, string, bool) (io.ReadCloser, error)
}

type Client struct {
	origin string
	token  string
	http   *http.Client
}

func NewClient(origin, token string, client *http.Client) (*Client, error) {
	normalized, err := endpointurl.NormalizeOrigin(origin, "Log archive URL")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(token) == "" || strings.ContainsAny(token, "\r\n") || client == nil {
		return nil, errors.New("log archive token and HTTP client are required")
	}
	return &Client{origin: normalized, token: token, http: client}, nil
}

func (c *Client) Open(ctx context.Context, namespace, jobUID string, timestamps bool) (io.ReadCloser, error) {
	if !validIdentity(namespace, jobUID) {
		return nil, ErrNotFound
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.origin+Path+namespace+"/"+jobUID, nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	if timestamps {
		request.URL.RawQuery = "timestamps=true"
	}
	// Archive credentials must never be forwarded to a redirected origin.
	client := *c.http
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read archived logs: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		if response.StatusCode == http.StatusNotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("read archived logs: HTTP %d", response.StatusCode)
	}
	return response.Body, nil
}

func NewHandler(store *Store, token string) (http.Handler, error) {
	if store == nil || strings.TrimSpace(token) == "" || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("job log store and read token are required")
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Cache-Control", "no-store")
		if subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		identity := strings.Split(strings.TrimPrefix(request.URL.Path, Path), "/")
		if len(identity) != 2 {
			http.NotFound(writer, request)
			return
		}
		stream, record, err := store.Open(identity[0], identity[1])
		if errors.Is(err, ErrNotFound) {
			http.NotFound(writer, request)
			return
		}
		if err != nil {
			http.Error(writer, "unable to read archived logs", http.StatusInternalServerError)
			return
		}
		defer stream.Close()
		writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		if !record.Complete {
			defer func() { _, _ = io.WriteString(writer, IncompleteWarning) }()
		}
		if request.URL.Query().Get("timestamps") == "true" {
			_, _ = io.Copy(writer, stream)
			return
		}
		scanner := bufio.NewScanner(stream)
		scanner.Buffer(make([]byte, 32*1024), maxLineBytes)
		for scanner.Scan() {
			timestamp, content, found := strings.Cut(scanner.Text(), " ")
			if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil || !found {
				content = scanner.Text()
			}
			if _, err := io.WriteString(writer, content+"\n"); err != nil {
				return
			}
		}
	}), nil
}
