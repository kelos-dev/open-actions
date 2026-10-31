package artifact

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/kelos-dev/open-actions/internal/joblog"
)

func (s *Server) jobLogs(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	if s.logs == nil {
		http.NotFound(writer, request)
		return
	}
	token := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if request.Method == http.MethodPut && request.URL.Path == joblog.Path {
		claims, err := s.tokens.ParseRuntimeToken(token, s.now())
		if err != nil {
			http.Error(writer, "invalid log upload credential", http.StatusUnauthorized)
			return
		}
		var chunk joblog.Chunk
		if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 2*joblog.MaxChunkBytes)).Decode(&chunk); err != nil {
			http.Error(writer, "invalid log chunk", http.StatusBadRequest)
			return
		}
		err = s.logs.Append(claims.WorkflowJobBackendID, chunk)
		switch {
		case errors.Is(err, joblog.ErrConflict):
			http.Error(writer, err.Error(), http.StatusConflict)
		case errors.Is(err, joblog.ErrLimit):
			http.Error(writer, err.Error(), http.StatusRequestEntityTooLarge)
		case err != nil:
			s.logger.Error("job log upload failed", "job_uid", claims.WorkflowJobBackendID, "error", err)
			http.Error(writer, "job log upload failed", http.StatusInternalServerError)
		default:
			writer.WriteHeader(http.StatusNoContent)
		}
		return
	}
	parts := strings.Split(strings.TrimPrefix(request.URL.Path, joblog.Path), "/")
	if request.Method != http.MethodGet || len(parts) != 2 || parts[0] == "" {
		http.NotFound(writer, request)
		return
	}
	if !s.logReads.Validate(parts[0], token, s.now()) {
		http.Error(writer, "invalid log read credential", http.StatusUnauthorized)
		return
	}
	sequence, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		http.Error(writer, "invalid log sequence", http.StatusBadRequest)
		return
	}
	chunk, err := s.logs.Read(parts[0], sequence)
	if errors.Is(err, joblog.ErrNotFound) {
		http.NotFound(writer, request)
		return
	}
	if err != nil {
		s.logger.Error("job log read failed", "job_uid", parts[0], "error", err)
		http.Error(writer, "job log read failed", http.StatusInternalServerError)
		return
	}
	writeJSON(writer, http.StatusOK, chunk)
}
