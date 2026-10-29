package joblog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

const (
	maxPendingBytes = 4 << 20
	FlushTimeout    = 30 * time.Second
)

// Uploader spools output locally until the service acknowledges it. A full
// spool applies backpressure. Archive errors are reported by Close so that
// container output and workflow cleanup can continue after archiving fails.
type Uploader struct {
	ctx             context.Context
	outputContext   context.Context
	stopOutputWatch func() bool
	cancel          context.CancelFunc
	client          *Client
	token           string
	file            *os.File
	mu              sync.Mutex
	changed         *sync.Cond
	written         int64
	acknowledged    int64
	closing         bool
	progress        uint64
	maxBlocked      time.Duration
	err             error
	discardErr      error
	wake            chan struct{}
	done            chan struct{}
}

func NewUploader(ctx, outputContext context.Context, client *Client, token, directory string) (*Uploader, error) {
	if ctx == nil || outputContext == nil || client == nil || token == "" || directory == "" {
		return nil, errors.New("log upload context, client, credential, and spool directory are required")
	}
	file, err := os.CreateTemp(directory, ".open-actions-log-")
	if err != nil {
		return nil, fmt.Errorf("create log spool: %w", err)
	}
	retryContext, cancel := context.WithCancel(ctx)
	u := &Uploader{ctx: retryContext, outputContext: outputContext, cancel: cancel, client: client, token: token, file: file, wake: make(chan struct{}, 1), done: make(chan struct{}), maxBlocked: 30 * time.Second}
	u.changed = sync.NewCond(&u.mu)
	u.stopOutputWatch = context.AfterFunc(outputContext, func() { u.mu.Lock(); u.changed.Broadcast(); u.mu.Unlock() })
	go u.upload()
	return u, nil
}

func (u *Uploader) signal() {
	select {
	case u.wake <- struct{}{}:
	default:
	}
}

func (u *Uploader) Write(data []byte) (int, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	accepted := len(data)
	written := 0
	for len(data) > 0 {
		for u.written-u.acknowledged >= maxPendingBytes && u.err == nil && !u.closing {
			if err := u.outputContext.Err(); err != nil {
				u.discardErr = fmt.Errorf("log archive spool remained full when execution stopped: %w", err)
				break
			}
			progress := u.progress
			deadline := time.AfterFunc(u.maxBlocked, func() {
				u.mu.Lock()
				defer u.mu.Unlock()
				if u.err == nil && u.progress == progress && u.written-u.acknowledged >= maxPendingBytes {
					u.err = errors.New("log archive spool remained full beyond its wait limit")
					u.cancel()
					u.changed.Broadcast()
				}
			})
			u.changed.Wait()
			deadline.Stop()
		}
		if u.err != nil || u.discardErr != nil {
			return accepted, nil
		}
		if u.closing {
			return written, io.ErrClosedPipe
		}
		position := u.written % maxPendingBytes
		length := min(len(data), maxPendingBytes-int(u.written-u.acknowledged), maxPendingBytes-int(position))
		n, err := u.file.WriteAt(data[:length], position)
		u.written += int64(n)
		written += n
		data = data[n:]
		if err != nil {
			u.err = err
			u.signal()
			return accepted, nil
		}
		if u.written-u.acknowledged >= MaxChunkBytes {
			u.signal()
		}
	}
	return written, nil
}

func (u *Uploader) Close() error {
	deadline := time.AfterFunc(FlushTimeout, u.cancel)
	defer deadline.Stop()
	defer u.cancel()
	u.mu.Lock()
	u.closing = true
	u.changed.Broadcast()
	u.mu.Unlock()
	u.signal()
	<-u.done
	u.mu.Lock()
	defer u.mu.Unlock()
	return errors.Join(u.err, u.discardErr)
}

func (u *Uploader) upload() {
	defer close(u.done)
	defer u.stopOutputWatch()
	defer u.cancel()
	defer os.Remove(u.file.Name())
	defer u.file.Close()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var sequence uint64
	for {
		flushPartial := false
		select {
		case <-ticker.C:
			flushPartial = true
		case <-u.wake:
		case <-u.ctx.Done():
			u.mu.Lock()
			if u.err == nil {
				u.err = fmt.Errorf("archive runner logs: %w", u.ctx.Err())
			}
			u.changed.Broadcast()
			u.mu.Unlock()
			return
		}
		for {
			u.mu.Lock()
			if u.err != nil {
				u.changed.Broadcast()
				u.mu.Unlock()
				return
			}
			length := min(u.written-u.acknowledged, MaxChunkBytes)
			finished := u.closing && u.acknowledged+length == u.written
			if length == 0 && (!finished || u.discardErr != nil) {
				u.mu.Unlock()
				if finished {
					return
				}
				break
			}
			if length < MaxChunkBytes && !u.closing && !flushPartial {
				u.mu.Unlock()
				break
			}
			complete := finished && u.discardErr == nil
			data := make([]byte, length)
			position := u.acknowledged % maxPendingBytes
			first := min(length, maxPendingBytes-position)
			_, err := u.file.ReadAt(data[:first], position)
			if err == nil && first < length {
				_, err = u.file.ReadAt(data[first:], 0)
			}
			u.mu.Unlock()
			if err == nil {
				err = u.send(Chunk{Sequence: sequence, Data: data, Complete: complete})
			}
			u.mu.Lock()
			if err != nil {
				if u.err == nil {
					u.err = fmt.Errorf("archive runner logs: %w", err)
				}
				u.changed.Broadcast()
				u.mu.Unlock()
				return
			}
			u.acknowledged += length
			u.progress++
			flushPartial = false
			sequence++
			if u.acknowledged == u.written {
				if err := u.file.Truncate(0); err != nil {
					u.err = err
				}
				u.written, u.acknowledged = 0, 0
			}
			u.changed.Broadcast()
			u.mu.Unlock()
			if finished {
				return
			}
		}
	}
}

func (u *Uploader) send(chunk Chunk) error {
	backoff := time.Second
	for {
		ctx, cancel := context.WithTimeout(u.ctx, 5*time.Second)
		err := u.client.Append(ctx, u.token, chunk)
		cancel()
		if err == nil {
			return nil
		}
		var response *serviceError
		if errors.As(err, &response) && response.status < 500 && response.status != 408 && response.status != 429 {
			return err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-u.ctx.Done():
			timer.Stop()
			return errors.Join(err, u.ctx.Err())
		case <-timer.C:
		}
		backoff = min(2*backoff, 5*time.Second)
	}
}

// TimestampWriter preserves line timestamps when output is read without CRI.
// A single writer serializes stdout, stderr, and runner lifecycle records.
type TimestampWriter struct {
	mu        sync.Mutex
	target    io.Writer
	lineStart bool
}

func NewTimestampWriter(target io.Writer) *TimestampWriter {
	return &TimestampWriter{target: target, lineStart: true}
}

func (w *TimestampWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	written := 0
	for index := 0; index < len(data); index++ {
		if w.lineStart {
			if _, err := io.WriteString(w.target, time.Now().UTC().Format(time.RFC3339Nano)+" "); err != nil {
				return written, err
			}
			w.lineStart = false
		}
		if data[index] == '\n' || index == len(data)-1 {
			n, err := w.target.Write(data[written : index+1])
			written += n
			if err != nil {
				return written, err
			}
			w.lineStart = data[index] == '\n'
		}
	}
	return written, nil
}
