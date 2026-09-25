package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nelsonlaidev/uprest/internal/redisproxy"
	"github.com/nelsonlaidev/uprest/internal/scriptnorm"
	"github.com/nelsonlaidev/uprest/internal/upstash"
)

const maxBodySize = 10 << 20
const commandTimeout = 30 * time.Second
const syncTokenHeader = "Upstash-Sync-Token" //nolint:gosec // G101: HTTP header name, not a credential

var requestSequence atomic.Uint64

type requestIDContextKey struct{}

type apiHandler struct {
	pools      *redisproxy.Manager
	logger     *slog.Logger
	normalizer *scriptnorm.Normalizer
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func NewHandler(pools *redisproxy.Manager, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	return &apiHandler{
		pools:      pools,
		logger:     logger,
		normalizer: scriptnorm.New(),
	}
}

func (h *apiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	requestID := newRequestID()
	r = r.WithContext(context.WithValue(r.Context(), requestIDContextKey{}, requestID))
	response := &statusWriter{ResponseWriter: w}

	defer func() {
		duration := time.Since(started)
		status := response.status

		if status == 0 {
			status = http.StatusOK
		}

		h.logger.InfoContext(r.Context(), "request completed",
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"duration_ms", float64(duration.Microseconds())/1000,
		)
	}()

	h.serve(response, r)
}

func (h *apiHandler) serve(w http.ResponseWriter, r *http.Request) {
	token, ok := bearerToken(r)

	if len(r.Header.Values("Authorization")) == 0 {
		token = r.URL.Query().Get("_token")
		ok = token != ""
	}

	if !ok || !h.pools.Authorized(token) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "Unauthorized"})
		return
	}

	w.Header().Set(syncTokenHeader, newRequestID())

	if unsupportedEndpoint(r.URL.Path) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "Not Found"})
		return
	}

	client, release, err := h.pools.Acquire(token)

	if errors.Is(err, redisproxy.ErrUnauthorized) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "Unauthorized"})
		return
	}

	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "Service Unavailable"})
		return
	}

	defer release()

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/":
		h.serveBodyCommand(w, r, client)

	case r.Method == http.MethodPost && r.URL.Path == "/pipeline":
		h.serveBatch(w, r, client, false)

	case r.Method == http.MethodPost && r.URL.Path == "/multi-exec":
		h.serveBatch(w, r, client, true)

	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && !reservedPath(r.URL.Path):
		h.servePathCommand(w, r, client)

	default:
		w.Header().Set("Allow", allowedMethods(r.URL.Path))
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method Not Allowed"})
	}
}

func (h *apiHandler) serveBodyCommand(w http.ResponseWriter, r *http.Request, client *redis.Client) {
	command, err := upstash.DecodeCommand(requestBody(w, r))

	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Invalid command"})
		return
	}

	h.executeCommand(w, r, client, command)
}

func (h *apiHandler) servePathCommand(w http.ResponseWriter, r *http.Request, client *redis.Client) {
	var body []byte

	if r.Method == http.MethodPost {
		var err error
		body, err = io.ReadAll(requestBody(w, r))

		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Invalid command"})
			return
		}
	}

	command, err := upstash.DecodePathCommand(r.URL.EscapedPath(), r.URL.RawQuery, body)

	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Invalid command"})
		return
	}

	h.executeCommand(w, r, client, command)
}

func (h *apiHandler) executeCommand(w http.ResponseWriter, r *http.Request, client *redis.Client, command []any) {
	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)

	defer cancel()

	h.normalizeCommand(r, command)

	result, err := client.Do(ctx, command...).Result()
	response, status := commandResponse(result, err, responseUsesBase64(r))

	writeJSON(w, status, response)
}

func (h *apiHandler) serveBatch(w http.ResponseWriter, r *http.Request, client *redis.Client, transaction bool) {
	commands, err := upstash.DecodePipeline(requestBody(w, r))

	if err != nil {
		message := "Invalid pipeline"

		if transaction {
			message = "Invalid transaction"
		}

		writeJSON(w, http.StatusBadRequest, map[string]any{"error": message})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)

	defer cancel()

	var pipeline redis.Pipeliner

	if transaction {
		pipeline = client.TxPipeline()
	} else {
		pipeline = client.Pipeline()
	}

	results := make([]*redis.Cmd, len(commands))

	for i, command := range commands {
		h.normalizeCommand(r, command)
		results[i] = pipeline.Do(ctx, command...)
	}

	_, err = pipeline.Exec(ctx)

	var redisError redis.Error

	if err != nil && !errors.Is(err, redis.Nil) && !errors.As(err, &redisError) {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": "Redis unavailable"})
		return
	}

	if transaction && err != nil && strings.HasPrefix(err.Error(), "EXECABORT") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}

	base64Response := responseUsesBase64(r)
	response := make([]map[string]any, len(results))

	for i, result := range results {
		item, status := commandResponse(result.Val(), result.Err(), base64Response)

		if status == http.StatusBadGateway {
			writeJSON(w, status, item)
			return
		}

		response[i] = item
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *apiHandler) normalizeCommand(r *http.Request, command []any) {
	if !h.normalizer.NormalizeCommand(command) {
		return
	}

	name, _ := command[0].(string)
	message := "unsupported Lua flags removed"

	if strings.EqualFold(name, "EVALSHA") || strings.EqualFold(name, "EVALSHA_RO") {
		message = "normalized script hash alias applied"
	}

	h.logger.DebugContext(r.Context(), message,
		"request_id", r.Context().Value(requestIDContextKey{}),
		"command", strings.ToUpper(name),
	)
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}

	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}

	return w.ResponseWriter.Write(body)
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func bearerToken(r *http.Request) (string, bool) {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")

	return token, ok && token != "" && strings.EqualFold(scheme, "Bearer")
}

func newRequestID() string {
	var value [16]byte

	if _, err := rand.Read(value[:]); err == nil {
		return hex.EncodeToString(value[:])
	}

	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatUint(requestSequence.Add(1), 36)
}

func requestBody(w http.ResponseWriter, r *http.Request) io.Reader {
	return http.MaxBytesReader(w, r.Body, maxBodySize)
}

func reservedPath(path string) bool {
	return path == "/" || path == "/pipeline" || path == "/multi-exec"
}

func unsupportedEndpoint(path string) bool {
	trimmed := strings.TrimPrefix(path, "/")
	segment, _, _ := strings.Cut(trimmed, "/")

	switch strings.ToLower(segment) {
	case "monitor", "psubscribe", "subscribe":
		return true
	}

	return false
}

func allowedMethods(path string) string {
	if reservedPath(path) {
		return http.MethodPost
	}

	return http.MethodGet + ", " + http.MethodPost
}

func responseUsesBase64(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upstash-Encoding"), "base64")
}

func commandResponse(result any, err error, base64Response bool) (map[string]any, int) {
	if errors.Is(err, redis.Nil) {
		result = nil
	} else if err != nil {
		var redisError redis.Error

		if errors.As(err, &redisError) {
			return map[string]any{"error": err.Error()}, http.StatusBadRequest
		}

		return map[string]any{"error": "Redis unavailable"}, http.StatusBadGateway
	}

	if base64Response {
		result = upstash.EncodeBase64Result(result)
	}

	return map[string]any{"result": result}, http.StatusOK
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)

	if err != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":"Internal Server Error"}`)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_, _ = w.Write(append(body, '\n'))
}
