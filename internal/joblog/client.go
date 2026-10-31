package joblog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type serviceError struct{ status int }

func (e *serviceError) Error() string { return fmt.Sprintf("persist job log: HTTP %d", e.status) }

type Client struct {
	origin string
	http   *http.Client
}

func NewClient(origin string, httpClient *http.Client) (*Client, error) {
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" || httpClient == nil {
		return nil, errors.New("log service HTTP origin and client are required")
	}
	return &Client{origin: strings.TrimSuffix(origin, "/"), http: httpClient}, nil
}

func (c *Client) Append(ctx context.Context, token string, chunk Chunk) error {
	data, err := json.Marshal(chunk)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, c.origin+Path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return &serviceError{status: response.StatusCode}
	}
	return nil
}

func (c *Client) Read(ctx context.Context, job string, sequence uint64, token string) (Chunk, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.origin+Path+url.PathEscape(job)+"/"+strconv.FormatUint(sequence, 10), nil)
	if err != nil {
		return Chunk{}, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := c.http.Do(request)
	if err != nil {
		return Chunk{}, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return Chunk{}, ErrNotFound
	}
	if response.StatusCode != http.StatusOK {
		return Chunk{}, fmt.Errorf("read job %q log: HTTP %d", job, response.StatusCode)
	}
	var chunk Chunk
	err = json.NewDecoder(io.LimitReader(response.Body, 2*MaxChunkBytes)).Decode(&chunk)
	if err == nil && (chunk.Sequence != sequence || len(chunk.Data) > MaxChunkBytes) {
		err = errors.New("log service returned an invalid chunk")
	}
	return chunk, err
}
