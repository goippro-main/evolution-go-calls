// Command whatsapp-call-e2e validates one real paired WhatsApp audio call.
// It never creates or pairs an instance: pairing remains an explicit operator action.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	rate         = 16000
	frameSamples = 960
	frameBytes   = frameSamples * 2
)

type cfg struct {
	api, instance, key, signing, listen, webhook, out, wav string
	mode, number, outboundMode                             string
	duration, delay, timeout                               time.Duration
	maxCalls                                               int
	configure                                              bool
}

type wsMessage struct {
	Event      string         `json:"event"`
	CallID     string         `json:"callId,omitempty"`
	SampleRate int            `json:"sampleRate,omitempty"`
	Track      string         `json:"track,omitempty"`
	Payload    string         `json:"payload,omitempty"`
	Reason     string         `json:"reason,omitempty"`
	Phase      string         `json:"phase,omitempty"`
	Direction  string         `json:"direction,omitempty"`
	Stats      map[string]any `json:"stats,omitempty"`
}

type offer struct {
	ID      string
	Creator string
}

type readResult struct {
	message wsMessage
	err     error
}

type logger struct {
	mu  sync.Mutex
	enc *json.Encoder
}

type recorder struct {
	mu                                                           sync.Mutex
	file                                                         *os.File
	path                                                         string
	samples, frames, nonZeroFrames, silentFrames, nonZeroSamples int64
	peak                                                         int16
	first, last                                                  time.Time
}

func main() {
	var c cfg
	flag.StringVar(&c.api, "api", env("EVOLUTION_API_URL", "http://127.0.0.1:4000"), "Evolution API base URL")
	flag.StringVar(&c.instance, "instance", os.Getenv("EVOLUTION_INSTANCE"), "instance id/name")
	flag.StringVar(&c.key, "apikey", os.Getenv("EVOLUTION_INSTANCE_API_KEY"), "instance token; never logged")
	flag.StringVar(&c.signing, "signing-key", os.Getenv("CALL_STREAM_SIGNING_KEY"), "stream HMAC key")
	flag.StringVar(&c.listen, "webhook-listen", env("E2E_WEBHOOK_LISTEN", "127.0.0.1:8090"), "webhook listen address")
	flag.StringVar(&c.webhook, "webhook-url", os.Getenv("E2E_WEBHOOK_URL"), "webhook URL reachable by Evolution")
	flag.StringVar(&c.out, "out-dir", env("E2E_OUT_DIR", "/tmp/evolution-go-call-e2e"), "artifact directory")
	flag.StringVar(&c.outboundMode, "outbound-mode", os.Getenv("E2E_OUTBOUND_MODE"), "outbound marker source: speech, tone, or wav; default speech unless -outbound-wav is set")
	flag.StringVar(&c.wav, "outbound-wav", "", "optional 16 kHz mono PCM16 WAV; selects wav mode when -outbound-mode is omitted")
	flag.StringVar(&c.mode, "mode", "inbound", "call direction: inbound or outbound")
	flag.StringVar(&c.number, "number", "", "ordinary WhatsApp number for outbound call")
	flag.DurationVar(&c.duration, "duration", 8*time.Second, "stream duration")
	flag.DurationVar(&c.delay, "inject-delay", 750*time.Millisecond, "delay before marker injection")
	flag.DurationVar(&c.timeout, "http-timeout", 10*time.Second, "HTTP timeout")
	flag.IntVar(&c.maxCalls, "max-calls", 0, "inbound calls to process before exiting; 0 means keep listening until interrupted")
	flag.BoolVar(&c.configure, "configure", true, "configure /instance/connect with this webhook")
	flag.Parse()
	if c.instance == "" || c.key == "" {
		log.Fatal("set -instance/EVOLUTION_INSTANCE and -apikey/EVOLUTION_INSTANCE_API_KEY")
	}
	if c.mode != "inbound" && c.mode != "outbound" {
		log.Fatal("-mode must be inbound or outbound")
	}
	if c.mode == "outbound" && strings.TrimSpace(c.number) == "" {
		log.Fatal("-number is required in outbound mode")
	}
	c.outboundMode = resolvedOutboundMode(c.outboundMode, c.wav)
	if c.outboundMode != "speech" && c.outboundMode != "tone" && c.outboundMode != "wav" {
		log.Fatal("-outbound-mode must be speech, tone, or wav")
	}
	if c.outboundMode == "wav" && c.wav == "" {
		log.Fatal("-outbound-wav is required when -outbound-mode=wav")
	}
	if c.outboundMode != "wav" && c.wav != "" {
		log.Fatal("-outbound-wav can only be used with -outbound-mode=wav")
	}
	if c.webhook == "" {
		c.webhook = "http://" + c.listen + "/webhook"
	}
	if err := os.MkdirAll(c.out, 0750); err != nil {
		log.Fatal(err)
	}
	logFile, err := os.OpenFile(filepath.Join(c.out, "lifecycle.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()
	l := &logger{enc: json.NewEncoder(logFile)}
	l.write("harness_started", map[string]any{"api": c.api, "instance": c.instance, "mode": c.mode, "webhook": c.webhook, "streamAuth": authMode(c.signing), "outDir": c.out})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	offers := make(chan offer, 1)
	server := &http.Server{Addr: c.listen, ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleWebhook(w, r, offers, l)
	})}
	listener, err := net.Listen("tcp", c.listen)
	if err != nil {
		l.write("webhook_listen_failed", map[string]any{"listen": c.listen, "error": err.Error()})
		log.Fatal(err)
	}
	l.write("webhook_listening", map[string]any{"listen": listener.Addr().String(), "url": c.webhook})
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			l.write("webhook_server_error", map[string]any{"error": err.Error()})
		}
		l.write("webhook_server_stopped", nil)
	}()
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(stopCtx)
	}()

	client := &http.Client{Timeout: c.timeout}
	if c.configure {
		if err := post(client, c.api+"/instance/connect", c.key, map[string]any{"webhookUrl": c.webhook, "subscribe": []string{"CALL"}, "immediate": true}); err != nil {
			l.write("configure_failed", map[string]any{"error": err.Error()})
			log.Fatal(err)
		}
		l.write("configure_succeeded", nil)
	}
	if err := runCalls(ctx, client, c, offers, l); err != nil {
		log.Fatal(err)
	}
}

func runCalls(ctx context.Context, client *http.Client, c cfg, offers <-chan offer, l *logger) error {
	processed := 0
	for {
		if c.mode == "outbound" && processed > 0 {
			return nil
		}
		if c.mode == "inbound" && c.maxCalls > 0 && processed >= c.maxCalls {
			l.write("max_calls_reached", map[string]any{"maxCalls": c.maxCalls})
			return nil
		}
		callID, err := prepareCall(ctx, client, c, offers, l)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			l.write("call_prepare_failed", map[string]any{"error": err.Error()})
			if c.mode == "outbound" {
				return err
			}
			continue
		}
		processed++
		l.write("call_attempt_started", map[string]any{"callId": callID, "attempt": processed})
		hangupDone, streamErr := stream(client, ctx, c, callID, l)
		if streamErr != nil {
			l.write("stream_failed", map[string]any{"callId": callID, "error": streamErr.Error()})
			log.Printf("stream: %v", streamErr)
		}
		if !hangupDone {
			if err := post(client, c.api+"/call/hangup", c.key, map[string]string{"callId": callID}); err != nil {
				l.write("hangup_failed", map[string]any{"callId": callID, "error": err.Error()})
				log.Printf("hangup: %v", err)
			} else {
				l.write("hangup_succeeded", map[string]any{"callId": callID})
			}
		}
		if streamErr != nil {
			l.write("call_attempt_failed", map[string]any{"callId": callID, "attempt": processed, "error": streamErr.Error()})
			if c.mode == "outbound" {
				return streamErr
			}
			continue
		}
		l.write("call_attempt_succeeded", map[string]any{"callId": callID, "attempt": processed})
	}
}

func prepareCall(ctx context.Context, client *http.Client, c cfg, offers <-chan offer, l *logger) (string, error) {
	if c.mode == "outbound" {
		var response struct {
			CallID string `json:"callId"`
		}
		if err := postJSON(client, c.api+"/call/dial", c.key, map[string]string{"number": c.number}, &response); err != nil {
			l.write("dial_failed", map[string]any{"error": err.Error()})
			return "", err
		}
		if response.CallID == "" {
			return "", errors.New("/call/dial returned no callId")
		}
		l.write("dial_succeeded", map[string]any{"callId": response.CallID, "number": c.number})
		return response.CallID, nil
	}

	log.Printf("waiting for CallOffer; webhook=%s", c.webhook)
	select {
	case <-ctx.Done():
		l.write("harness_cancelled", nil)
		return "", ctx.Err()
	case incoming := <-offers:
		if incoming.ID == "" || incoming.Creator == "" {
			return "", errors.New("CallOffer did not contain call id and creator")
		}
		l.write("call_offer_observed", map[string]any{"callId": incoming.ID, "callCreator": incoming.Creator})
		if err := post(client, c.api+"/call/answer", c.key, map[string]string{"callId": incoming.ID, "callCreator": incoming.Creator}); err != nil {
			l.write("answer_failed", map[string]any{"callId": incoming.ID, "error": err.Error()})
			return "", err
		}
		l.write("answer_succeeded", map[string]any{"callId": incoming.ID})
		return incoming.ID, nil
	}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func authMode(key string) string {
	if key != "" {
		return "hmac"
	}
	return "apikey"
}

func resolvedOutboundMode(mode, wav string) string {
	if mode != "" {
		return mode
	}
	if wav != "" {
		return "wav"
	}
	return "speech"
}

func (l *logger) write(event string, fields map[string]any) {
	record := map[string]any{"ts": time.Now().UTC().Format(time.RFC3339Nano), "event": event}
	for key, value := range fields {
		record[key] = value
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = l.enc.Encode(record)
}

func handleWebhook(w http.ResponseWriter, r *http.Request, offers chan<- offer, l *logger) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		l.write("webhook_invalid_json", map[string]any{"error": err.Error()})
		return
	}
	event, _ := find(payload, "event")
	l.write("webhook_event", map[string]any{"name": event})
	if !strings.EqualFold(event, "CallOffer") {
		w.WriteHeader(http.StatusOK)
		return
	}
	o := offer{ID: first(payload, "CallID", "callId", "callID"), Creator: first(payload, "CallCreator", "callCreator", "from", "From")}
	select {
	case offers <- o:
	default:
		l.write("call_offer_ignored", map[string]any{"callId": o.ID, "reason": "first-offer-already-selected"})
	}
	w.WriteHeader(http.StatusOK)
}

func first(root map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := find(root, key); ok && value != "" {
			return value
		}
	}
	return ""
}

func find(value any, wanted string) (string, bool) {
	switch current := value.(type) {
	case map[string]any:
		for key, item := range current {
			if strings.EqualFold(key, wanted) {
				if text, ok := item.(string); ok {
					return text, true
				}
			}
		}
		for _, item := range current {
			if text, ok := find(item, wanted); ok {
				return text, true
			}
		}
	case []any:
		for _, item := range current {
			if text, ok := find(item, wanted); ok {
				return text, true
			}
		}
	}
	return "", false
}

func post(client *http.Client, endpoint, key string, body any) error {
	return postJSON(client, endpoint, key, body, nil)
}

func postJSON(client *http.Client, endpoint, key string, body any, result any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(http.MethodPost, strings.TrimRight(endpoint, "/"), strings.NewReader(string(raw)))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("apikey", key)
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	bodyBytes, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("%s: %s: %s", endpoint, response.Status, strings.TrimSpace(string(bodyBytes)))
	}
	if result != nil && len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, result); err != nil {
			return fmt.Errorf("%s: invalid JSON response: %w", endpoint, err)
		}
	}
	return nil
}

func stream(client *http.Client, ctx context.Context, c cfg, callID string, l *logger) (bool, error) {
	streamURL, err := streamURL(c, callID)
	if err != nil {
		return false, err
	}
	conn, _, err := websocket.DefaultDialer.Dial(streamURL, nil)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	l.write("ws_connected", map[string]any{"callId": callID, "auth": authMode(c.signing)})
	rec, err := newRecorder(filepath.Join(c.out, "inbound-"+callID+".wav"))
	if err != nil {
		return false, err
	}
	defer func() {
		rec.close()
		l.write("audio_summary", rec.summary())
	}()
	outboundMode := resolvedOutboundMode(c.outboundMode, c.wav)
	source, sourceName, err := outbound(outboundMode, c.wav)
	if err != nil {
		return false, err
	}
	outboundPath := filepath.Join(c.out, "outbound-"+callID+".wav")
	if err := writeWAV(outboundPath, source); err != nil {
		return false, err
	}
	l.write("outbound_source", map[string]any{"name": sourceName, "mode": outboundMode, "wav": outboundPath, "samples": len(source), "durationMs": float64(len(source)) * 1000 / rate})
	var writeMu sync.Mutex
	write := func(message wsMessage) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return conn.WriteJSON(message)
	}
	if err := write(wsMessage{Event: "start", CallID: callID, SampleRate: rate}); err != nil {
		return false, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	reads := make(chan readResult, 1)
	go readMessages(conn, reads)
	ready := make(chan struct{})
	var readyOnce sync.Once
	markReady := func(reason string) {
		readyOnce.Do(func() {
			l.write("media_ready", map[string]any{"callId": callID, "reason": reason})
			close(ready)
		})
	}
	go inject(streamCtx, ready, write, source, c.delay, l)

	deadline := time.NewTimer(c.duration)
	defer deadline.Stop()
	var hangupDone bool
	var serverStats map[string]any
	deadlineC := deadline.C
	var closeDeadlineC <-chan time.Time
	finish := func() (bool, error) {
		if err := validateMedia(c.mode, rec.summary(), serverStats); err != nil {
			l.write("media_validation_fail", map[string]any{"callId": callID, "mode": c.mode, "error": err.Error(), "server": serverStats})
			return hangupDone, err
		}
		l.write("media_validation_pass", map[string]any{"callId": callID, "mode": c.mode, "server": serverStats})
		return hangupDone, nil
	}
	for {
		var message wsMessage
		select {
		case <-streamCtx.Done():
			return false, streamCtx.Err()
		case <-deadlineC:
			l.write("stream_duration_elapsed", map[string]any{"callId": callID})
			if err := post(client, c.api+"/call/hangup", c.key, map[string]string{"callId": callID}); err != nil {
				return false, fmt.Errorf("hangup at stream deadline: %w", err)
			}
			hangupDone = true
			l.write("hangup_succeeded", map[string]any{"callId": callID})
			deadlineC = nil
			closeTimer := time.NewTimer(2 * time.Second)
			defer closeTimer.Stop()
			closeDeadlineC = closeTimer.C
			continue
		case <-closeDeadlineC:
			return finish()
		case result := <-reads:
			if result.err == nil {
				message = result.message
				break
			}
			err := result.err
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				l.write("ws_closed", map[string]any{"callId": callID, "error": err.Error()})
				return finish()
			}
			l.write("stream_read_error", map[string]any{"callId": callID, "error": err.Error()})
			return false, err
		}
		switch message.Event {
		case "start":
			l.write("ws_start", map[string]any{"callId": message.CallID, "sampleRate": message.SampleRate})
		case "state":
			l.write("call_state", map[string]any{"callId": callID, "phase": message.Phase, "direction": message.Direction})
			if message.Phase == "active" && c.mode == "inbound" {
				markReady("active")
			}
		case "peer_accept":
			l.write("peer_accept", map[string]any{"callId": callID, "direction": message.Direction})
			if c.mode == "outbound" {
				markReady("peer_accept")
			}
		case "media_ready":
			l.write("media_ready_signal", map[string]any{"callId": callID, "direction": message.Direction})
			if c.mode == "inbound" {
				markReady("media_ready")
			}
		case "media":
			if message.Track != "inbound" {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(message.Payload)
			if err != nil || len(raw) != frameBytes {
				return false, fmt.Errorf("invalid inbound PCM frame: bytes=%d err=%v", len(raw), err)
			}
			if err := rec.add(raw); err != nil {
				return false, err
			}
			summary := rec.summary()
			l.write("inbound_frame", map[string]any{"callId": callID, "frameTs": time.Now().UTC().Format(time.RFC3339Nano), "frame": summary["inboundFrames"], "nonZeroFrames": summary["nonZeroFrames"], "silentFrames": summary["silentFrames"]})
		case "diagnostics":
			serverStats = message.Stats
			l.write("server_diagnostics", map[string]any{"callId": callID, "stats": serverStats})
		case "stop":
			l.write("ws_stop", map[string]any{"callId": callID, "reason": message.Reason})
			return finish()
		}
	}
}

func readMessages(conn *websocket.Conn, out chan<- readResult) {
	for {
		var message wsMessage
		if err := conn.ReadJSON(&message); err != nil {
			out <- readResult{err: err}
			return
		}
		out <- readResult{message: message}
	}
}

func inject(ctx context.Context, ready <-chan struct{}, write func(wsMessage) error, source []int16, delay time.Duration, l *logger) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ready:
	case <-ctx.Done():
		return
	}
	select {
	case <-ready:
	case <-ctx.Done():
		return
	}
	var frames, nonZeroFrames, nonZeroSamples, peak int64
	start := time.Now().UTC()
	for offset := 0; ; offset += frameSamples {
		payload := make([]byte, frameBytes)
		nonZero := false
		for index := 0; index < frameSamples; index++ {
			sample := source[(offset+index)%len(source)]
			if sample > 8 || sample < -8 {
				nonZero = true
				nonZeroSamples++
			}
			abs := int64(sample)
			if abs < 0 {
				abs = -abs
			}
			if abs > peak {
				peak = abs
			}
			binary.LittleEndian.PutUint16(payload[index*2:], uint16(sample))
		}
		if nonZero {
			nonZeroFrames++
		}
		if err := write(wsMessage{Event: "media", Track: "outbound", SampleRate: rate, Payload: base64.StdEncoding.EncodeToString(payload)}); err != nil {
			return
		}
		frames++
		select {
		case <-time.After(60 * time.Millisecond):
		case <-ctx.Done():
			l.write("outbound_summary", map[string]any{"frames": frames, "nonZeroFrames": nonZeroFrames, "nonZeroSamples": nonZeroSamples, "peak": peak, "firstFrameTs": start, "lastFrameTs": time.Now().UTC()})
			return
		}
	}
}

func validateMedia(mode string, inbound, server map[string]any) error {
	if positiveCounter(inbound, "inboundFrames") == 0 || positiveCounter(inbound, "nonZeroFrames") == 0 {
		return fmt.Errorf("%s media failed: recorder has no non-zero inbound audio (frames=%d nonZeroFrames=%d)", mode, positiveCounter(inbound, "inboundFrames"), positiveCounter(inbound, "nonZeroFrames"))
	}
	if server == nil {
		return errors.New("media failed: server diagnostics were not received")
	}
	if positiveCounter(server, "outboundFrames") == 0 || positiveCounter(server, "outboundNonZeroFrames") == 0 {
		return fmt.Errorf("%s media failed: bridge injected no non-zero outbound audio (frames=%d nonZeroFrames=%d)", mode, positiveCounter(server, "outboundFrames"), positiveCounter(server, "outboundNonZeroFrames"))
	}
	return nil
}

func positiveCounter(values map[string]any, key string) int64 {
	switch value := values[key].(type) {
	case int64:
		return value
	case float64:
		return int64(value)
	case int:
		return int64(value)
	default:
		return 0
	}
}

func timeoutError(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func streamURL(c cfg, callID string) (string, error) {
	u, err := url.Parse(strings.TrimRight(c.api, "/") + "/call/stream/" + url.PathEscape(callID))
	if err != nil {
		return "", err
	}
	if u.Scheme == "http" {
		u.Scheme = "ws"
	} else if u.Scheme == "https" {
		u.Scheme = "wss"
	}
	query := u.Query()
	if c.signing == "" {
		query.Set("apikey", c.key)
	} else {
		exp := time.Now().Add(4 * time.Minute).Unix()
		message := fmt.Sprintf("%s|%s|%d", c.instance, callID, exp)
		mac := hmac.New(sha256.New, []byte(c.signing))
		_, _ = mac.Write([]byte(message))
		query.Set("instance", c.instance)
		query.Set("exp", fmt.Sprint(exp))
		query.Set("token", fmt.Sprintf("%x", mac.Sum(nil)))
	}
	u.RawQuery = query.Encode()
	return u.String(), nil
}

func newRecorder(path string) (*recorder, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return nil, err
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	r := &recorder{file: file, path: path}
	if _, err := file.Write(make([]byte, 44)); err != nil {
		_ = file.Close()
		return nil, err
	}
	return r, nil
}

func (r *recorder) add(raw []byte) error {
	if _, err := r.file.Write(raw); err != nil {
		return err
	}
	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frames++
	r.samples += int64(len(raw) / 2)
	if r.first.IsZero() {
		r.first = now
	}
	r.last = now
	nonZero := false
	for i := 0; i < len(raw); i += 2 {
		sample := int16(binary.LittleEndian.Uint16(raw[i:]))
		abs := sample
		if abs < 0 {
			abs = -abs
		}
		if abs > 8 {
			nonZero = true
			r.nonZeroSamples++
		}
		if abs > r.peak {
			r.peak = abs
		}
	}
	if nonZero {
		r.nonZeroFrames++
	} else {
		r.silentFrames++
	}
	return nil
}

func (r *recorder) summary() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return map[string]any{
		"wav": r.path, "inboundFrames": r.frames, "samples": r.samples,
		"durationMs":    float64(r.samples) * 1000 / rate,
		"nonZeroFrames": r.nonZeroFrames, "silentFrames": r.silentFrames,
		"nonZeroSamples": r.nonZeroSamples, "peak": r.peak,
		"firstFrameTs": r.first, "lastFrameTs": r.last,
		"mediaEvidence": r.frames > 0 && r.nonZeroFrames > 0,
	}
}

func (r *recorder) close() {
	if r.file == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	dataBytes := uint32(r.samples * 2)
	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], 36+dataBytes)
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1)
	binary.LittleEndian.PutUint16(header[22:24], 1)
	binary.LittleEndian.PutUint32(header[24:28], rate)
	binary.LittleEndian.PutUint32(header[28:32], rate*2)
	binary.LittleEndian.PutUint16(header[32:34], 2)
	binary.LittleEndian.PutUint16(header[34:36], 16)
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], dataBytes)
	_, _ = r.file.WriteAt(header, 0)
	_ = r.file.Close()
	r.file = nil
}

func outbound(mode, path string) ([]int16, string, error) {
	switch mode {
	case "speech":
		return generatedSpeechMarker(), "generated-speech-marker: this is a whatsapp voice bridge test from goippro", nil
	case "tone":
		samples := make([]int16, rate*3)
		frequencies := []float64{440, 880, 660}
		for i := range samples {
			frequency := frequencies[(i/(rate/3))%len(frequencies)]
			envelope := 0.32
			if i%(rate/10) < rate/100 {
				envelope = 0
			}
			samples[i] = int16(math.Sin(2*math.Pi*frequency*float64(i)/rate) * envelope * 32767)
		}
		return padToFrame(samples), "generated-marker-440-880-660Hz", nil
	case "wav":
		samples, err := readPCM16WAV(path)
		if err != nil {
			return nil, "", err
		}
		return padToFrame(samples), path, nil
	default:
		return nil, "", fmt.Errorf("unknown outbound mode %q", mode)
	}
}

func readPCM16WAV(path string) ([]int16, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < 44 || string(raw[:4]) != "RIFF" || string(raw[8:12]) != "WAVE" {
		return nil, errors.New("outbound WAV is not RIFF/WAVE")
	}
	if binary.LittleEndian.Uint16(raw[22:24]) != 1 || binary.LittleEndian.Uint32(raw[24:28]) != rate || binary.LittleEndian.Uint16(raw[34:36]) != 16 {
		return nil, errors.New("outbound WAV must be mono PCM16 16 kHz")
	}
	offset := bytesIndex(raw, []byte("data"), 36)
	if offset < 0 || offset+8 > len(raw) {
		return nil, errors.New("outbound WAV has no data chunk")
	}
	size := int(binary.LittleEndian.Uint32(raw[offset+4:]))
	start := offset + 8
	if size > len(raw)-start || size == 0 {
		return nil, errors.New("outbound WAV data chunk is invalid")
	}
	raw = raw[start : start+size]
	samples := make([]int16, len(raw)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
	}
	return samples, nil
}

type speechPart struct {
	kind string
	ms   int
	vow  string
	amp  float64
}

func generatedSpeechMarker() []int16 {
	// Fixed, synthetic, offline phrase: "this is a WhatsApp voice bridge test from GoIPpro".
	// It is intentionally robotic and impersonal: a deterministic marker, not a cloned voice.
	parts := []speechPart{
		{kind: "silence", ms: 180},
		{kind: "noise", ms: 55, amp: 0.045}, {kind: "vowel", vow: "ih", ms: 95, amp: 0.23}, {kind: "noise", ms: 45, amp: 0.055},
		{kind: "silence", ms: 40},
		{kind: "vowel", vow: "ih", ms: 85, amp: 0.21}, {kind: "noise", ms: 60, amp: 0.045},
		{kind: "silence", ms: 60},
		{kind: "vowel", vow: "uh", ms: 80, amp: 0.20}, {kind: "vowel", vow: "aa", ms: 115, amp: 0.24}, {kind: "noise", ms: 65, amp: 0.055},
		{kind: "silence", ms: 35},
		{kind: "vowel", vow: "ae", ms: 95, amp: 0.24}, {kind: "stop", ms: 70, amp: 0.05},
		{kind: "silence", ms: 90},
		{kind: "vowel", vow: "oy", ms: 120, amp: 0.24}, {kind: "noise", ms: 55, amp: 0.045},
		{kind: "silence", ms: 55},
		{kind: "stop", ms: 45, amp: 0.05}, {kind: "vowel", vow: "ih", ms: 75, amp: 0.22}, {kind: "vowel", vow: "ih", ms: 70, amp: 0.18}, {kind: "noise", ms: 55, amp: 0.055},
		{kind: "silence", ms: 65},
		{kind: "noise", ms: 50, amp: 0.045}, {kind: "vowel", vow: "eh", ms: 100, amp: 0.24}, {kind: "noise", ms: 60, amp: 0.055},
		{kind: "silence", ms: 75},
		{kind: "noise", ms: 55, amp: 0.05}, {kind: "vowel", vow: "ah", ms: 105, amp: 0.23}, {kind: "vowel", vow: "ah", ms: 65, amp: 0.16},
		{kind: "silence", ms: 80},
		{kind: "vowel", vow: "ow", ms: 105, amp: 0.24}, {kind: "vowel", vow: "ay", ms: 90, amp: 0.22},
		{kind: "silence", ms: 45},
		{kind: "stop", ms: 45, amp: 0.055}, {kind: "vowel", vow: "iy", ms: 95, amp: 0.24},
		{kind: "silence", ms: 45},
		{kind: "stop", ms: 45, amp: 0.055}, {kind: "vowel", vow: "ow", ms: 130, amp: 0.24},
		{kind: "silence", ms: 420},
	}
	var samples []int16
	phase := 0.0
	for _, part := range parts {
		switch part.kind {
		case "vowel":
			generated, nextPhase := synthVowel(part.vow, part.ms, part.amp, phase)
			phase = nextPhase
			samples = append(samples, generated...)
		case "noise":
			samples = append(samples, synthNoise(part.ms, part.amp)...)
		case "stop":
			samples = append(samples, synthStop(part.ms, part.amp)...)
		default:
			samples = append(samples, make([]int16, msSamples(part.ms))...)
		}
	}
	return padToFrame(samples)
}

func synthVowel(name string, ms int, amp, phase float64) ([]int16, float64) {
	formants := map[string][3]float64{
		"aa": {730, 1090, 2440}, "ae": {660, 1720, 2410}, "ah": {640, 1190, 2390},
		"ay": {500, 1800, 2550}, "eh": {530, 1840, 2480}, "ih": {390, 1990, 2550},
		"iy": {270, 2290, 3010}, "ow": {570, 840, 2410}, "oy": {480, 1500, 2520},
		"uh": {440, 1020, 2240},
	}
	ff, ok := formants[name]
	if !ok {
		ff = formants["ah"]
	}
	count := msSamples(ms)
	out := make([]int16, count)
	for i := range out {
		t := float64(i) / rate
		x := float64(i) / float64(maxInt(1, count-1))
		env := math.Sin(math.Pi * x)
		if env < 0 {
			env = 0
		}
		pitch := 125.0 - 18.0*x
		phase += 2 * math.Pi * pitch / rate
		glottal := 0.62*math.Sin(phase) + 0.22*math.Sin(2*phase) + 0.08*math.Sin(3*phase)
		resonance := 0.34*math.Sin(2*math.Pi*ff[0]*t) + 0.19*math.Sin(2*math.Pi*ff[1]*t) + 0.10*math.Sin(2*math.Pi*ff[2]*t)
		value := (0.72*glottal + resonance) * env * amp
		out[i] = clampPCM(value)
	}
	return out, math.Mod(phase, 2*math.Pi)
}

func synthNoise(ms int, amp float64) []int16 {
	out := make([]int16, msSamples(ms))
	var state uint32 = 0x5eed1234
	var previous float64
	for i := range out {
		state = state*1664525 + 1013904223
		white := (float64(int32(state>>1)%20001) / 10000.0) - 1.0
		previous = 0.72*previous + 0.28*white
		x := float64(i) / float64(maxInt(1, len(out)-1))
		env := math.Sin(math.Pi * x)
		out[i] = clampPCM(previous * env * amp)
	}
	return out
}

func synthStop(ms int, amp float64) []int16 {
	out := make([]int16, msSamples(ms))
	burst := synthNoise(minInt(ms, 18), amp)
	copy(out, burst)
	return out
}

func msSamples(ms int) int {
	return int(math.Round(float64(ms) * rate / 1000))
}

func padToFrame(samples []int16) []int16 {
	if len(samples) == 0 || len(samples)%frameSamples == 0 {
		return samples
	}
	padded := make([]int16, len(samples)+(frameSamples-len(samples)%frameSamples))
	copy(padded, samples)
	return padded
}

func writeWAV(path string, samples []int16) error {
	if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	dataBytes := uint32(len(samples) * 2)
	header := make([]byte, 44)
	copy(header[0:4], "RIFF")
	binary.LittleEndian.PutUint32(header[4:8], 36+dataBytes)
	copy(header[8:12], "WAVE")
	copy(header[12:16], "fmt ")
	binary.LittleEndian.PutUint32(header[16:20], 16)
	binary.LittleEndian.PutUint16(header[20:22], 1)
	binary.LittleEndian.PutUint16(header[22:24], 1)
	binary.LittleEndian.PutUint32(header[24:28], rate)
	binary.LittleEndian.PutUint32(header[28:32], rate*2)
	binary.LittleEndian.PutUint16(header[32:34], 2)
	binary.LittleEndian.PutUint16(header[34:36], 16)
	copy(header[36:40], "data")
	binary.LittleEndian.PutUint32(header[40:44], dataBytes)
	if _, err := file.Write(header); err != nil {
		return err
	}
	for _, sample := range samples {
		var raw [2]byte
		binary.LittleEndian.PutUint16(raw[:], uint16(sample))
		if _, err := file.Write(raw[:]); err != nil {
			return err
		}
	}
	return nil
}

func clampPCM(value float64) int16 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	if value > 0.95 {
		value = 0.95
	}
	if value < -0.95 {
		value = -0.95
	}
	return int16(math.Round(value * 32767))
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func bytesIndex(haystack, needle []byte, start int) int {
	for i := start; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == string(needle) {
			return i
		}
	}
	return -1
}
