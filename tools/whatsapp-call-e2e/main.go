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
	mode, number                                           string
	duration, delay, timeout                               time.Duration
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
	flag.StringVar(&c.wav, "outbound-wav", "", "optional 16 kHz mono PCM16 WAV; default is marker tone")
	flag.StringVar(&c.mode, "mode", "inbound", "call direction: inbound or outbound")
	flag.StringVar(&c.number, "number", "", "ordinary WhatsApp number for outbound call")
	flag.DurationVar(&c.duration, "duration", 8*time.Second, "stream duration")
	flag.DurationVar(&c.delay, "inject-delay", 750*time.Millisecond, "delay before marker injection")
	flag.DurationVar(&c.timeout, "http-timeout", 10*time.Second, "HTTP timeout")
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
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			l.write("webhook_server_error", map[string]any{"error": err.Error()})
		}
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
	callID, err := prepareCall(ctx, client, c, offers, l)
	if err != nil {
		log.Fatal(err)
	}
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
		log.Fatal(streamErr)
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
	source, sourceName, err := outbound(c.wav)
	if err != nil {
		return false, err
	}
	l.write("outbound_source", map[string]any{"name": sourceName})
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
	var closeDeadline *time.Timer
	finish := func() (bool, error) {
		if err := validateMedia(c.mode, rec.summary(), serverStats); err != nil {
			l.write("media_validation_fail", map[string]any{"callId": callID, "mode": c.mode, "error": err.Error(), "server": serverStats})
			return hangupDone, err
		}
		l.write("media_validation_pass", map[string]any{"callId": callID, "mode": c.mode, "server": serverStats})
		return hangupDone, nil
	}
	for {
		select {
		case <-streamCtx.Done():
			return false, streamCtx.Err()
		case <-deadline.C:
			l.write("stream_duration_elapsed", map[string]any{"callId": callID})
			if err := post(client, c.api+"/call/hangup", c.key, map[string]string{"callId": callID}); err != nil {
				return false, fmt.Errorf("hangup at stream deadline: %w", err)
			}
			hangupDone = true
			l.write("hangup_succeeded", map[string]any{"callId": callID})
			closeDeadline = time.NewTimer(2 * time.Second)
		default:
		}
		if closeDeadline != nil {
			select {
			case <-closeDeadline.C:
				return finish()
			default:
			}
		}
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		var message wsMessage
		if err := conn.ReadJSON(&message); err != nil {
			if timeoutError(err) {
				continue
			}
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return finish()
			}
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

func outbound(path string) ([]int16, string, error) {
	if path == "" {
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
		return samples, "generated-marker-440-880-660Hz", nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	if len(raw) < 44 || string(raw[:4]) != "RIFF" || string(raw[8:12]) != "WAVE" {
		return nil, "", errors.New("outbound WAV is not RIFF/WAVE")
	}
	if binary.LittleEndian.Uint16(raw[22:24]) != 1 || binary.LittleEndian.Uint32(raw[24:28]) != rate || binary.LittleEndian.Uint16(raw[34:36]) != 16 {
		return nil, "", errors.New("outbound WAV must be mono PCM16 16 kHz")
	}
	offset := bytesIndex(raw, []byte("data"), 36)
	if offset < 0 || offset+8 > len(raw) {
		return nil, "", errors.New("outbound WAV has no data chunk")
	}
	size := int(binary.LittleEndian.Uint32(raw[offset+4:]))
	start := offset + 8
	if size > len(raw)-start || size == 0 {
		return nil, "", errors.New("outbound WAV data chunk is invalid")
	}
	raw = raw[start : start+size]
	samples := make([]int16, len(raw)/2)
	for i := range samples {
		samples[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
	}
	return samples, path, nil
}

func bytesIndex(haystack, needle []byte, start int) int {
	for i := start; i+len(needle) <= len(haystack); i++ {
		if string(haystack[i:i+len(needle)]) == string(needle) {
			return i
		}
	}
	return -1
}
