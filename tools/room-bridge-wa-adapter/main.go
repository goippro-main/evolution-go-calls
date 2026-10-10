// Command room-bridge-wa-adapter adapts the Evolution Go WhatsApp call stream
// to the existing Voximplant-style room_bridge.js WebSocket protocol.
//
// It does not answer calls, create calls, change launchd jobs, or deploy
// anything. Both WebSocket endpoints must be passed explicitly.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	sampleRate   = 16000
	frameSamples = 960
	frameBytes   = frameSamples * 2
)

type config struct {
	evolutionURL      string
	roomBridgeURL     string
	callID            string
	dir               string
	roomPrefix        string
	allowLegacyAPIKey bool
	lockDir           string
	readTimeout       time.Duration
	writeTimeout      time.Duration
	pingInterval      time.Duration
	vadThreshold      int
	prespeechFrames   int
	maxQueuedFrames   int
	maxRoomReconnects int
}

type runtimeConfig struct {
	evolutionURL *url.URL
	roomURL      *url.URL
	callID       string
	roomID       string
	lockPath     string
}

type evolutionMessage struct {
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

type roomMessage struct {
	Event          string     `json:"event"`
	SequenceNumber string     `json:"sequenceNumber,omitempty"`
	Start          *roomStart `json:"start,omitempty"`
	Media          *roomMedia `json:"media,omitempty"`
	Stop           any        `json:"stop,omitempty"`
}

type roomStart struct {
	StreamSid   string          `json:"streamSid"`
	AccountSid  string          `json:"accountSid"`
	CallSid     string          `json:"callSid"`
	MediaFormat roomMediaFormat `json:"mediaFormat"`
}

type roomMediaFormat struct {
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sampleRate"`
	Channels   int    `json:"channels"`
}

type roomMedia struct {
	Track     string `json:"track,omitempty"`
	Chunk     string `json:"chunk,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
	Payload   string `json:"payload"`
}

type adapter struct {
	c       config
	rt      runtimeConfig
	dialer  websocket.Dialer
	evoConn *websocket.Conn

	cancel context.CancelFunc

	evoOut chan evolutionMessage

	roomMu          sync.Mutex
	roomConn        *websocket.Conn
	roomSeq         int64
	roomChunk       int64
	roomTimestampMS int64
	roomReconnects  int

	speechStarted atomic.Bool
	prespeechMu   sync.Mutex
	prespeech     [][]byte
	stats         adapterStats
}

type adapterStats struct {
	evolutionInboundFrames  atomic.Int64
	roomInboundFrames       atomic.Int64
	roomOutboundFrames      atomic.Int64
	evolutionOutboundFrames atomic.Int64
	droppedEvolutionFrames  atomic.Int64
}

func main() {
	mode := "adapter"
	c := config{}
	oc := orchestratorConfig{}
	flag.StringVar(&mode, "mode", "adapter", "run mode: adapter or orchestrator")
	flag.StringVar(&c.evolutionURL, "evolution-ws-url", "", "Evolution Go /call/stream/<callId> WS URL with signed HMAC query")
	flag.StringVar(&c.roomBridgeURL, "room-bridge-ws-url", "", "loopback/tunnel WS URL for room_bridge.js, e.g. ws://127.0.0.1:8191")
	flag.StringVar(&c.callID, "call-id", "", "optional explicit callId; default is parsed from Evolution stream path")
	flag.StringVar(&c.dir, "dir", "pt2ru", "room_bridge.js translation direction")
	flag.StringVar(&c.roomPrefix, "room-prefix", "wa-call-", "prefix for callId-scoped room IDs")
	flag.BoolVar(&c.allowLegacyAPIKey, "allow-legacy-apikey", false, "allow Evolution apikey query instead of signed HMAC query")
	flag.StringVar(&c.lockDir, "lock-dir", os.TempDir(), "local lock directory used to prevent duplicate adapter owners for one callId")
	flag.DurationVar(&c.readTimeout, "read-timeout", 30*time.Second, "WebSocket read idle timeout")
	flag.DurationVar(&c.writeTimeout, "write-timeout", 5*time.Second, "WebSocket write timeout")
	flag.DurationVar(&c.pingInterval, "ping-interval", 10*time.Second, "WebSocket ping interval")
	flag.IntVar(&c.vadThreshold, "vad-threshold", 300, "RMS threshold before connecting to room_bridge.js and starting Gemini cost")
	flag.IntVar(&c.prespeechFrames, "prespeech-frames", 5, "silent preroll frames flushed when speech starts")
	flag.IntVar(&c.maxQueuedFrames, "max-queued-frames", 100, "outbound frame queue size toward Evolution")
	flag.IntVar(&c.maxRoomReconnects, "max-room-reconnects", 2, "room_bridge.js reconnect attempts after disconnect")
	flag.StringVar(&oc.apiBaseURL, "api", os.Getenv("EVOLUTION_API_URL"), "Evolution Go HTTP API base URL; orchestrator mode only")
	flag.StringVar(&oc.instance, "instance", os.Getenv("EVOLUTION_INSTANCE"), "Evolution instance id/name; orchestrator mode only")
	flag.StringVar(&oc.apiKey, "apikey", os.Getenv("EVOLUTION_INSTANCE_API_KEY"), "Evolution instance token; orchestrator mode only, never logged")
	flag.StringVar(&oc.signingKey, "signing-key", os.Getenv("CALL_STREAM_SIGNING_KEY"), "CALL_STREAM_SIGNING_KEY for HMAC stream URL signing; orchestrator mode only")
	flag.StringVar(&oc.webhookListen, "webhook-listen", "127.0.0.1:8090", "local webhook listen address; orchestrator mode only")
	flag.StringVar(&oc.webhookURL, "webhook-url", "", "webhook URL reachable by Evolution; defaults to listen URL when possible")
	flag.StringVar(&oc.rollbackWebhookURL, "rollback-webhook-url", "", "previous harness webhook URL restored after the one-call window")
	flag.StringVar(&oc.allowedCallersCSV, "allow-caller", "", "comma-separated exact allowed CallCreator JIDs/numbers for the one inbound call")
	flag.BoolVar(&oc.configureCutover, "configure-cutover", false, "temporarily point /instance/connect at this orchestrator and rollback afterward")
	flag.DurationVar(&oc.offerTimeout, "offer-timeout", 2*time.Minute, "maximum time to wait for the allowlisted CallOffer")
	flag.DurationVar(&oc.callTimeLimit, "call-time-limit", 90*time.Second, "maximum answered-call bridge duration before hangup")
	flag.DurationVar(&oc.httpTimeout, "http-timeout", 10*time.Second, "Evolution API HTTP timeout")
	flag.DurationVar(&oc.streamTokenTTL, "stream-token-ttl", 2*time.Minute, "short-lived HMAC stream URL TTL")
	flag.Parse()

	ctx, stop := signalContext()
	defer stop()
	var err error
	switch mode {
	case "adapter":
		err = run(ctx, c)
	case "orchestrator":
		oc.adapterConfig = c
		err = runControlledInboundOrchestrator(ctx, oc)
	default:
		err = fmt.Errorf("-mode must be adapter or orchestrator, got %q", mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func run(ctx context.Context, c config) error {
	rt, err := validateConfig(c)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	unlock, err := acquireCallLock(rt.lockPath)
	if err != nil {
		return err
	}
	defer unlock()

	a := &adapter{
		c:      c,
		rt:     rt,
		dialer: *websocket.DefaultDialer,
		cancel: cancel,
		evoOut: make(chan evolutionMessage, c.maxQueuedFrames),
	}
	log.Printf("adapter starting callId=%s room=%s evolution=%s roomBridge=%s", rt.callID, rt.roomID, redactURL(rt.evolutionURL), redactURL(rt.roomURL))
	defer func() {
		log.Printf("adapter stopped callId=%s inbound=%d roomIn=%d roomOut=%d outbound=%d droppedOutbound=%d",
			rt.callID,
			a.stats.evolutionInboundFrames.Load(),
			a.stats.roomInboundFrames.Load(),
			a.stats.roomOutboundFrames.Load(),
			a.stats.evolutionOutboundFrames.Load(),
			a.stats.droppedEvolutionFrames.Load(),
		)
	}()
	return a.run(ctx)
}

func (a *adapter) run(ctx context.Context) error {
	evo, _, err := a.dialer.DialContext(ctx, a.rt.evolutionURL.String(), nil)
	if err != nil {
		return fmt.Errorf("connect Evolution stream: %w", err)
	}
	a.evoConn = evo
	defer a.closeAll()

	configureConn(evo, a.c.readTimeout)
	go pingLoop(ctx, evo, a.c.pingInterval, a.c.writeTimeout)
	go a.evolutionWriter(ctx)

	for {
		var msg evolutionMessage
		if err := evo.ReadJSON(&msg); err != nil {
			a.cancel()
			return err
		}
		if err := a.handleEvolutionMessage(ctx, msg); err != nil {
			a.cancel()
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (a *adapter) handleEvolutionMessage(ctx context.Context, msg evolutionMessage) error {
	switch msg.Event {
	case "start", "state", "media_ready":
		if msg.CallID != "" && msg.CallID != a.rt.callID {
			return fmt.Errorf("Evolution stream callId changed from %q to %q", a.rt.callID, msg.CallID)
		}
		return nil
	case "stop":
		a.cancel()
		return nil
	case "media":
		if msg.Track != "inbound" {
			return nil
		}
		if msg.SampleRate != 0 && msg.SampleRate != sampleRate {
			return fmt.Errorf("unsupported Evolution sampleRate %d", msg.SampleRate)
		}
		raw, err := decodeFramePayload(msg.Payload)
		if err != nil {
			return err
		}
		a.stats.evolutionInboundFrames.Add(1)
		if !a.speechStarted.Load() {
			if a.c.vadThreshold > 0 && rmsPCM16(raw) <= a.c.vadThreshold {
				a.rememberPrespeech(raw)
				return nil
			}
			a.speechStarted.Store(true)
			if err := a.connectRoom(ctx); err != nil {
				return err
			}
			for _, frame := range a.drainPrespeech() {
				if err := a.sendRoomMedia(frame); err != nil {
					return err
				}
			}
		}
		if err := a.connectRoom(ctx); err != nil {
			return err
		}
		return a.sendRoomMedia(raw)
	default:
		return nil
	}
}

func (a *adapter) rememberPrespeech(frame []byte) {
	if a.c.prespeechFrames <= 0 {
		return
	}
	a.prespeechMu.Lock()
	defer a.prespeechMu.Unlock()
	cp := append([]byte(nil), frame...)
	a.prespeech = append(a.prespeech, cp)
	if len(a.prespeech) > a.c.prespeechFrames {
		a.prespeech = a.prespeech[len(a.prespeech)-a.c.prespeechFrames:]
	}
}

func (a *adapter) drainPrespeech() [][]byte {
	a.prespeechMu.Lock()
	defer a.prespeechMu.Unlock()
	out := a.prespeech
	a.prespeech = nil
	return out
}

func (a *adapter) connectRoom(ctx context.Context) error {
	a.roomMu.Lock()
	defer a.roomMu.Unlock()
	if a.roomConn != nil && a.roomConn.UnderlyingConn() != nil {
		return nil
	}
	if a.roomReconnects > a.c.maxRoomReconnects {
		return errors.New("room_bridge reconnect budget exhausted")
	}
	a.roomReconnects++
	room, _, err := a.dialer.DialContext(ctx, a.rt.roomURL.String(), nil)
	if err != nil {
		return fmt.Errorf("connect room_bridge: %w", err)
	}
	configureConn(room, a.c.readTimeout)
	a.roomConn = room
	a.roomSeq = 1
	a.roomChunk = 0
	a.roomTimestampMS = 0
	if err := a.writeRoomJSON(roomMessage{
		Event:          "start",
		SequenceNumber: fmt.Sprint(a.roomSeq),
		Start: &roomStart{
			StreamSid:  "wa-" + a.rt.roomID,
			AccountSid: "evolution-go-adapter",
			CallSid:    a.rt.callID,
			MediaFormat: roomMediaFormat{
				Encoding:   "PCM16",
				SampleRate: sampleRate,
				Channels:   1,
			},
		},
	}); err != nil {
		_ = room.Close()
		a.roomConn = nil
		return err
	}
	a.roomSeq++
	go pingLoop(ctx, room, a.c.pingInterval, a.c.writeTimeout)
	go a.roomReader(ctx, room)
	return nil
}

func (a *adapter) sendRoomMedia(frame []byte) error {
	a.roomMu.Lock()
	defer a.roomMu.Unlock()
	if a.roomConn == nil {
		return errors.New("room_bridge is not connected")
	}
	a.roomChunk++
	a.roomTimestampMS += int64(frameSamples * 1000 / sampleRate)
	msg := roomMessage{
		Event:          "media",
		SequenceNumber: fmt.Sprint(a.roomSeq),
		Media: &roomMedia{
			Track:     "inbound",
			Chunk:     fmt.Sprint(a.roomChunk),
			Timestamp: fmt.Sprint(a.roomTimestampMS),
			Payload:   base64.StdEncoding.EncodeToString(frame),
		},
	}
	a.roomSeq++
	if err := a.writeRoomJSON(msg); err != nil {
		_ = a.roomConn.Close()
		a.roomConn = nil
		return err
	}
	a.stats.roomInboundFrames.Add(1)
	return nil
}

func (a *adapter) writeRoomJSON(msg roomMessage) error {
	if a.roomConn == nil {
		return errors.New("room_bridge websocket is nil")
	}
	_ = a.roomConn.SetWriteDeadline(time.Now().Add(a.c.writeTimeout))
	return a.roomConn.WriteJSON(msg)
}

func (a *adapter) roomReader(ctx context.Context, conn *websocket.Conn) {
	defer func() {
		a.roomMu.Lock()
		if a.roomConn == conn {
			a.roomConn = nil
		}
		a.roomMu.Unlock()
		_ = conn.Close()
	}()
	for {
		var msg roomMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return
		}
		if msg.Event != "media" || msg.Media == nil || msg.Media.Payload == "" {
			continue
		}
		if msg.Media.Track != "" && msg.Media.Track != "outbound" {
			continue
		}
		raw, err := base64.StdEncoding.DecodeString(msg.Media.Payload)
		if err != nil {
			continue
		}
		for _, frame := range splitPCMFrames(raw) {
			out := evolutionMessage{
				Event:      "media",
				CallID:     a.rt.callID,
				SampleRate: sampleRate,
				Track:      "outbound",
				Payload:    base64.StdEncoding.EncodeToString(frame),
			}
			select {
			case a.evoOut <- out:
				a.stats.roomOutboundFrames.Add(1)
			case <-ctx.Done():
				return
			default:
				a.stats.droppedEvolutionFrames.Add(1)
			}
		}
	}
}

func (a *adapter) evolutionWriter(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-a.evoOut:
			_ = a.evoConn.SetWriteDeadline(time.Now().Add(a.c.writeTimeout))
			if err := a.evoConn.WriteJSON(msg); err != nil {
				a.cancel()
				return
			}
			a.stats.evolutionOutboundFrames.Add(1)
		}
	}
}

func (a *adapter) closeAll() {
	a.roomMu.Lock()
	if a.roomConn != nil {
		_ = a.writeRoomJSON(roomMessage{Event: "stop"})
		_ = a.roomConn.Close()
		a.roomConn = nil
	}
	a.roomMu.Unlock()
	if a.evoConn != nil {
		_ = a.evoConn.Close()
	}
}

func configureConn(conn *websocket.Conn, readTimeout time.Duration) {
	conn.SetReadLimit(4 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		return nil
	})
}

func pingLoop(ctx context.Context, conn *websocket.Conn, interval, writeTimeout time.Duration) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := conn.WriteControl(websocket.PingMessage, []byte("ping"), time.Now().Add(writeTimeout)); err != nil {
				return
			}
		}
	}
}

func validateConfig(c config) (runtimeConfig, error) {
	if c.evolutionURL == "" || c.roomBridgeURL == "" {
		return runtimeConfig{}, errors.New("both -evolution-ws-url and -room-bridge-ws-url are required; no network defaults are used")
	}
	if c.maxQueuedFrames <= 0 {
		return runtimeConfig{}, errors.New("-max-queued-frames must be positive")
	}
	if c.maxRoomReconnects < 0 {
		return runtimeConfig{}, errors.New("-max-room-reconnects must be non-negative")
	}
	evoURL, err := parseWSURL(c.evolutionURL)
	if err != nil {
		return runtimeConfig{}, fmt.Errorf("Evolution URL: %w", err)
	}
	roomURL, err := parseWSURL(c.roomBridgeURL)
	if err != nil {
		return runtimeConfig{}, fmt.Errorf("room_bridge URL: %w", err)
	}
	if err := validateEvolutionAuth(evoURL, c.allowLegacyAPIKey); err != nil {
		return runtimeConfig{}, err
	}
	if !isLoopbackHost(roomURL.Hostname()) {
		return runtimeConfig{}, fmt.Errorf("room_bridge URL must be loopback/private tunnel, got host %q", roomURL.Hostname())
	}
	callID := c.callID
	if callID == "" {
		callID = path.Base(evoURL.Path)
	}
	if callID == "" || callID == "." || callID == "/" {
		return runtimeConfig{}, errors.New("callId is required or must be parseable from Evolution URL path")
	}
	if !strings.Contains(evoURL.Path, "/call/stream/") {
		return runtimeConfig{}, errors.New("Evolution URL must point at /call/stream/<callId>")
	}
	roomID := roomIDForCall(c.roomPrefix, callID)
	q := roomURL.Query()
	q.Set("room", roomID)
	q.Set("dir", c.dir)
	roomURL.RawQuery = q.Encode()
	lockPath := ""
	if c.lockDir != "" {
		sum := sha256.Sum256([]byte(callID))
		lockPath = path.Join(c.lockDir, "wa-room-bridge-"+hex.EncodeToString(sum[:8])+".lock")
	}
	return runtimeConfig{evolutionURL: evoURL, roomURL: roomURL, callID: callID, roomID: roomID, lockPath: lockPath}, nil
}

func parseWSURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, errors.New("scheme must be ws or wss")
	}
	if u.Hostname() == "" {
		return nil, errors.New("host is required")
	}
	if u.Fragment != "" {
		return nil, errors.New("URL fragments are not allowed")
	}
	return u, nil
}

func validateEvolutionAuth(u *url.URL, allowLegacy bool) error {
	q := u.Query()
	if q.Get("apikey") != "" {
		if allowLegacy {
			return nil
		}
		return errors.New("Evolution apikey query is rejected by default; use signed HMAC instance/exp/token URL or -allow-legacy-apikey for lab-only compatibility")
	}
	if q.Get("instance") == "" || q.Get("exp") == "" || q.Get("token") == "" {
		return errors.New("Evolution stream URL must include signed HMAC query params: instance, exp, token")
	}
	return nil
}

var roomSafe = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

func roomIDForCall(prefix, callID string) string {
	clean := roomSafe.ReplaceAllString(callID, "_")
	clean = strings.Trim(clean, "_.-")
	if clean == "" {
		sum := sha256.Sum256([]byte(callID))
		clean = hex.EncodeToString(sum[:8])
	}
	room := prefix + clean
	if len(room) <= 96 {
		return room
	}
	sum := sha256.Sum256([]byte(callID))
	return room[:79] + "-" + hex.EncodeToString(sum[:8])
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func redactURL(u *url.URL) string {
	cp := *u
	q := cp.Query()
	for _, key := range []string{"token", "apikey", "key"} {
		if q.Get(key) != "" {
			q.Set(key, "REDACTED")
		}
	}
	cp.RawQuery = q.Encode()
	return cp.String()
}

func acquireCallLock(lockPath string) (func(), error) {
	if lockPath == "" {
		return func() {}, nil
	}
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("another local adapter appears to own this call stream: %s", lockPath)
		}
		return nil, err
	}
	_, _ = fmt.Fprintf(file, "pid=%d\nstarted=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
	_ = file.Close()
	return func() { _ = os.Remove(lockPath) }, nil
}

func decodeFramePayload(payload string) ([]byte, error) {
	if payload == "" {
		return nil, errors.New("empty Evolution media payload")
	}
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, fmt.Errorf("bad Evolution media base64: %w", err)
	}
	if len(raw) != frameBytes {
		return nil, fmt.Errorf("Evolution media frame has %d bytes, want %d", len(raw), frameBytes)
	}
	return raw, nil
}

func splitPCMFrames(raw []byte) [][]byte {
	if len(raw) == 0 {
		return nil
	}
	frames := make([][]byte, 0, (len(raw)+frameBytes-1)/frameBytes)
	for len(raw) > 0 {
		n := frameBytes
		if len(raw) < n {
			n = len(raw)
		}
		frame := make([]byte, frameBytes)
		copy(frame, raw[:n])
		frames = append(frames, frame)
		raw = raw[n:]
	}
	return frames
}

func rmsPCM16(raw []byte) int {
	if len(raw) < 2 {
		return 0
	}
	var sum float64
	samples := len(raw) / 2
	for i := 0; i < samples; i++ {
		v := int16(binary.LittleEndian.Uint16(raw[i*2:]))
		sum += float64(v) * float64(v)
	}
	return int(math.Sqrt(sum / float64(samples)))
}
