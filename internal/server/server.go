package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
const streamHeartbeatInterval = 15 * time.Second
const streamWriteTimeout = 5 * time.Second
const monitorStopTimeout = 5 * time.Second
const monitorEventBufferSize = 100
const syncTokenHeader = "Upstash-Sync-Token" //nolint:gosec // G101: HTTP header name, not a credential
const responseFormatHeader = "Upstash-Response-Format"
const responseEncodingHeader = "Upstash-Encoding"
const invalidResponseFormatMessage = "Invalid response format"
const invalidResponseEncodingMessage = "Invalid response encoding"
const invalidSubscriptionMessage = "Invalid subscription"
const redisUnavailableMessage = "Redis unavailable"
const streamHeartbeat = ": keepalive\n\n"

var requestSequence atomic.Uint64
var errInvalidResponseFormat = errors.New(invalidResponseFormatMessage)     //nolint:staticcheck // ST1005: external HTTP API message.
var errInvalidResponseEncoding = errors.New(invalidResponseEncodingMessage) //nolint:staticcheck // ST1005: external HTTP API message.
var errInvalidSubscription = errors.New(invalidSubscriptionMessage)         //nolint:staticcheck // ST1005: external HTTP API message.
var errInvalidSubscriptionEvent = errors.New("subscription event contains a line break")
var errInvalidMonitorEvent = errors.New("monitor event contains a line break")
var errInvalidMonitorConfirmation = errors.New("invalid monitor confirmation")

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

	monitor := monitorRoute(r.URL.Path)
	patternSubscription, subscription := subscriptionRoute(r.URL.Path)
	var subscriptionNames []string
	var resp2Response bool
	var err error

	if monitor {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method Not Allowed"})
			return
		}
	} else if subscription {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method Not Allowed"})
			return
		}

		subscriptionNames, err = decodeSubscriptionNames(r.URL.EscapedPath())

		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
	} else {
		if unsupportedEndpoint(r.URL.Path) {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": "Not Found"})
			return
		}

		if r.Method != http.MethodPost && (r.Method != http.MethodGet || reservedPath(r.URL.Path)) {
			w.Header().Set("Allow", allowedMethods(r.URL.Path))
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method Not Allowed"})
			return
		}

		resp2Response, err = responseUsesRESP2(r)

		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
	}

	acquireCtx, cancelAcquire := context.WithTimeout(r.Context(), commandTimeout)
	var client *redis.Client
	var release func()

	if monitor {
		client, release, err = h.pools.AcquireDedicated(acquireCtx, token)
	} else {
		client, release, err = h.pools.Acquire(acquireCtx, token)
	}

	cancelAcquire()

	if errors.Is(err, redisproxy.ErrUnauthorized) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "Unauthorized"})
		return
	}

	if err != nil {
		if errors.Is(err, context.Canceled) && r.Context().Err() != nil {
			return
		}

		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "Service Unavailable"})
		return
	}

	if monitor {
		h.serveMonitor(w, r, client, release)
		return
	}

	defer release()

	if subscription {
		h.serveSubscription(w, r, client, subscriptionNames, patternSubscription)
		return
	}

	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/":
		h.serveBodyCommand(w, r, client, resp2Response)

	case r.Method == http.MethodPost && r.URL.Path == "/pipeline":
		h.serveBatch(w, r, client, false, resp2Response)

	case r.Method == http.MethodPost && r.URL.Path == "/multi-exec":
		h.serveBatch(w, r, client, true, false)

	case (r.Method == http.MethodGet || r.Method == http.MethodPost) && !reservedPath(r.URL.Path):
		h.servePathCommand(w, r, client, resp2Response)

	default:
		w.Header().Set("Allow", allowedMethods(r.URL.Path))
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method Not Allowed"})
	}
}

func (h *apiHandler) serveMonitor(w http.ResponseWriter, r *http.Request, client *redis.Client, release func()) {
	events := make(chan string, monitorEventBufferSize)
	// MonitorCmd starts a background reader after its initial Process call returns.
	// A sticky Conn keeps that socket out of the ordinary pool while the reader owns it.
	connection := client.Conn()
	monitor := connection.Monitor(r.Context(), events)

	if err := monitor.Err(); err != nil {
		_ = connection.Close()
		release()

		if r.Context().Err() == nil {
			response, status := commandResponse(nil, err, false)
			writeJSON(w, status, response)
		}

		return
	}

	monitor.Start()

	defer stopMonitor(release, monitor, events)

	confirmationTimer := time.NewTimer(commandTimeout)

	defer confirmationTimer.Stop()

	var confirmation string

	select {
	case <-r.Context().Done():
		return

	case <-confirmationTimer.C:
		if r.Context().Err() != nil {
			return
		}

		h.logger.DebugContext(r.Context(), "Redis monitor confirmation timed out",
			"request_id", r.Context().Value(requestIDContextKey{}),
			"timeout", commandTimeout,
		)
		writeRedisUnavailable(w)
		return

	case confirmation = <-events:
	}

	firstEvent, err := formatMonitorEvent(confirmation, true)

	if err != nil {
		writeRedisUnavailable(w)
		return
	}

	controller := http.NewResponseController(w)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	if err := writeSSE(w, controller, firstEvent); err != nil {
		return
	}

	heartbeat := time.NewTimer(streamHeartbeatInterval)

	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case event := <-events:
			body, err := formatMonitorEvent(event, false)

			if err != nil {
				return
			}

			if err := writeSSE(w, controller, body); err != nil {
				return
			}

			heartbeat.Reset(streamHeartbeatInterval)

		case <-heartbeat.C:
			if err := writeSSE(w, controller, []byte(streamHeartbeat)); err != nil {
				return
			}

			heartbeat.Reset(streamHeartbeatInterval)
		}
	}
}

func stopMonitor(release func(), monitor *redis.MonitorCmd, events <-chan string) {
	stopped := make(chan struct{})
	timer := time.NewTimer(monitorStopTimeout)

	defer timer.Stop()

	// MonitorCmd.Stop waits for its background reader's mutex. Releasing the
	// dedicated client interrupts an idle socket read, while draining events
	// prevents the reader from blocking on a full channel during shutdown. The
	// sticky Conn must not be returned to its parent pool while that reader may
	// still be active; closing the parent client already closes its socket.
	go func() {
		monitor.Stop()
		close(stopped)
	}()

	release()

	for {
		select {
		case <-events:

		case <-stopped:
			for {
				select {
				case <-events:

				default:
					return
				}
			}

		case <-timer.C:
			return
		}
	}
}

func (h *apiHandler) serveSubscription(w http.ResponseWriter, r *http.Request, client *redis.Client, names []string, pattern bool) {
	var pubsub *redis.PubSub

	if pattern {
		pubsub = client.PSubscribe(r.Context(), names...)
	} else {
		pubsub = client.Subscribe(r.Context(), names...)
	}

	defer func() {
		_ = pubsub.Close()
	}()

	confirmation, err := pubsub.ReceiveTimeout(r.Context(), commandTimeout)

	if err != nil {
		if r.Context().Err() == nil {
			writeRedisUnavailable(w)
		}

		return
	}

	firstEvent, err := formatSubscriptionEvent(confirmation)

	if err != nil || len(firstEvent) == 0 {
		writeRedisUnavailable(w)
		return
	}

	controller := http.NewResponseController(w)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	if err := writeSSE(w, controller, firstEvent); err != nil {
		return
	}

	events := pubsub.ChannelWithSubscriptions()
	heartbeat := time.NewTimer(streamHeartbeatInterval)

	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return

		case event, open := <-events:
			if !open {
				return
			}

			body, err := formatSubscriptionEvent(event)

			if err != nil {
				return
			}

			if len(body) == 0 {
				continue
			}

			if err := writeSSE(w, controller, body); err != nil {
				return
			}

			heartbeat.Reset(streamHeartbeatInterval)

		case <-heartbeat.C:
			if err := writeSSE(w, controller, []byte(streamHeartbeat)); err != nil {
				return
			}

			heartbeat.Reset(streamHeartbeatInterval)
		}
	}
}

func (h *apiHandler) serveBodyCommand(w http.ResponseWriter, r *http.Request, client *redis.Client, resp2Response bool) {
	command, err := upstash.DecodeCommand(requestBody(w, r))

	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Invalid command"})
		return
	}

	h.executeCommand(w, r, client, command, resp2Response)
}

func (h *apiHandler) servePathCommand(w http.ResponseWriter, r *http.Request, client *redis.Client, resp2Response bool) {
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

	h.executeCommand(w, r, client, command, resp2Response)
}

func (h *apiHandler) executeCommand(w http.ResponseWriter, r *http.Request, client *redis.Client, command []any, resp2Response bool) {
	ctx, cancel := context.WithTimeout(r.Context(), commandTimeout)

	defer cancel()

	h.normalizeCommand(r, command)

	if resp2Response {
		response, err := client.DoRaw(ctx, command...).Result()

		if err != nil {
			writeRedisUnavailable(w)
			return
		}

		status := http.StatusOK

		if len(response) > 0 && response[0] == '-' {
			status = http.StatusBadRequest
		}

		writeRESP2(w, status, response)
		return
	}

	result, err := client.Do(ctx, command...).Result()
	response, status := commandResponse(result, err, responseUsesBase64(r))

	writeJSON(w, status, response)
}

func (h *apiHandler) serveBatch(w http.ResponseWriter, r *http.Request, client *redis.Client, transaction bool, resp2Response bool) {
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

	if resp2Response {
		pipeline := client.Pipeline()
		results := make([]*redis.RawCmd, len(commands))

		for i, command := range commands {
			h.normalizeCommand(r, command)
			results[i] = redis.NewRawCmd(ctx, command...)
			_ = pipeline.Process(ctx, results[i]) // Pipeline.Process only queues commands and returns nil.
		}

		if _, err := pipeline.Exec(ctx); err != nil {
			writeRedisUnavailable(w)
			return
		}

		var response []byte

		for _, result := range results {
			item, err := result.Result()

			if err != nil {
				writeRedisUnavailable(w)
				return
			}

			response = append(response, item...)
		}

		writeRESP2(w, http.StatusOK, response)
		return
	}

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
		writeRedisUnavailable(w)
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

func monitorRoute(path string) bool {
	return strings.EqualFold(path, "/monitor")
}

func subscriptionRoute(path string) (pattern bool, ok bool) {
	trimmed := strings.TrimPrefix(path, "/")
	segment, _, _ := strings.Cut(trimmed, "/")

	switch strings.ToLower(segment) {
	case "subscribe":
		return false, true

	case "psubscribe":
		return true, true

	default:
		return false, false
	}
}

func decodeSubscriptionNames(escapedPath string) ([]string, error) {
	command, err := upstash.DecodePathCommand(escapedPath, "", nil)

	if err != nil || len(command) < 2 {
		return nil, errInvalidSubscription
	}

	names := make([]string, len(command)-1)

	for i, value := range command[1:] {
		name, ok := value.(string)

		if !ok || name == "" || containsSSELineBreak(name) {
			return nil, errInvalidSubscription
		}

		names[i] = name
	}

	return names, nil
}

func unsupportedEndpoint(path string) bool {
	trimmed := strings.TrimPrefix(path, "/")
	segment, _, _ := strings.Cut(trimmed, "/")

	return strings.EqualFold(segment, "monitor")
}

func allowedMethods(path string) string {
	if reservedPath(path) {
		return http.MethodPost
	}

	return http.MethodGet + ", " + http.MethodPost
}

func responseUsesBase64(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get(responseEncodingHeader), "base64")
}

func responseUsesRESP2(r *http.Request) (bool, error) {
	format := r.Header.Get(responseFormatHeader)

	if format == "" || strings.EqualFold(format, "json") {
		return false, nil
	}

	if !strings.EqualFold(format, "resp2") {
		return false, errInvalidResponseFormat
	}

	if responseUsesBase64(r) {
		return false, errInvalidResponseEncoding
	}

	return true, nil
}

func commandResponse(result any, err error, base64Response bool) (map[string]any, int) {
	if errors.Is(err, redis.Nil) {
		result = nil
	} else if err != nil {
		var redisError redis.Error

		if errors.As(err, &redisError) {
			return map[string]any{"error": err.Error()}, http.StatusBadRequest
		}

		return map[string]any{"error": redisUnavailableMessage}, http.StatusBadGateway
	}

	if base64Response {
		result = upstash.EncodeBase64Result(result)
	}

	return map[string]any{"result": result}, http.StatusOK
}

func formatSubscriptionEvent(event any) ([]byte, error) {
	switch value := event.(type) {
	case *redis.Subscription:
		if containsSSELineBreak(value.Kind, value.Channel) {
			return nil, errInvalidSubscriptionEvent
		}

		return fmt.Appendf(nil, "data: %s,%s,%d\n\n", value.Kind, value.Channel, value.Count), nil

	case *redis.Message:
		if containsSSELineBreak(value.Pattern, value.Channel, value.Payload) {
			return nil, errInvalidSubscriptionEvent
		}

		if value.Pattern != "" {
			return fmt.Appendf(nil, "data: pmessage,%s,%s,%s\n\n", value.Pattern, value.Channel, value.Payload), nil
		}

		return fmt.Appendf(nil, "data: message,%s,%s\n\n", value.Channel, value.Payload), nil

	default:
		return nil, nil
	}
}

func formatMonitorEvent(event string, confirmation bool) ([]byte, error) {
	if containsSSELineBreak(event) {
		return nil, errInvalidMonitorEvent
	}

	if confirmation {
		if event != "OK" {
			return nil, errInvalidMonitorConfirmation
		}

		return []byte("data: \"OK\"\n\n"), nil
	}

	return fmt.Appendf(nil, "data: %s\n\n", event), nil
}

func writeSSE(w http.ResponseWriter, controller *http.ResponseController, body []byte) error {
	deadlineSupported := true

	if err := controller.SetWriteDeadline(time.Now().Add(streamWriteTimeout)); err != nil {
		if !errors.Is(err, http.ErrNotSupported) {
			return err
		}

		deadlineSupported = false
	}

	if _, err := w.Write(body); err != nil {
		return err
	}

	if err := controller.Flush(); err != nil {
		return err
	}

	if deadlineSupported {
		return controller.SetWriteDeadline(time.Time{})
	}

	return nil
}

func containsSSELineBreak(values ...string) bool {
	for _, value := range values {
		if strings.ContainsAny(value, "\r\n") {
			return true
		}
	}

	return false
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

func writeRedisUnavailable(w http.ResponseWriter) {
	writeJSON(w, http.StatusBadGateway, map[string]any{"error": redisUnavailableMessage})
}

func writeRESP2(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(status)

	_, _ = w.Write(body) //nolint:gosec // G705: raw RESP2 bytes are intentionally proxied as application/octet-stream.
}
