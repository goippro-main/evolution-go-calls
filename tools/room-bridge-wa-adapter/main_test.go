package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var testUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func TestValidateConfigRequiresExplicitNetworkAndSignedEvolutionURL(t *testing.T) {
	if _, err := validateConfig(config{}); err == nil || !strings.Contains(err.Error(), "no network defaults") {
		t.Fatalf("expected no default network error, got %v", err)
	}
	baseRoom := "ws://127.0.0.1:8191"
	legacy := config{
		evolutionURL:    "ws://127.0.0.1:4000/call/stream/call-1?apikey=secret",
		roomBridgeURL:   baseRoom,
		dir:             "pt2ru",
		roomPrefix:      "wa-call-",
		maxQueuedFrames: 1,
	}
	if _, err := validateConfig(legacy); err == nil || !strings.Contains(err.Error(), "apikey") {
		t.Fatalf("expected apikey rejection, got %v", err)
	}
	signed := legacy
	signed.evolutionURL = "ws://127.0.0.1:4000/call/stream/call-1?instance=sala2&exp=1800000000&token=abc"
	rt, err := validateConfig(signed)
	if err != nil {
		t.Fatalf("signed URL should pass: %v", err)
	}
	if rt.callID != "call-1" || rt.roomID != "wa-call-call-1" {
		t.Fatalf("unexpected runtime config: %#v", rt)
	}
	if rt.roomURL.Query().Get("room") != "wa-call-call-1" || rt.roomURL.Query().Get("dir") != "pt2ru" {
		t.Fatalf("room query not set: %s", rt.roomURL.String())
	}
	publicRoom := signed
	publicRoom.roomBridgeURL = "ws://81.17.140.198:8191"
	if _, err := validateConfig(publicRoom); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("public room_bridge host must be rejected, got %v", err)
	}
}

func TestRoomIDForCallScopesAndSanitizesCallID(t *testing.T) {
	got := roomIDForCall("wa-call-", "007A/27 AF?secret=token")
	if got != "wa-call-007A_27_AF_secret_token" {
		t.Fatalf("room id = %q", got)
	}
	long := roomIDForCall("wa-call-", strings.Repeat("A", 200))
	if len(long) > 96 {
		t.Fatalf("long room id length = %d", len(long))
	}
}

func TestAdapterMapsEvolutionAndRoomBridgeProtocolsOffline(t *testing.T) {
	inbound := frameWithSample(1200)
	out1 := frameWithSample(2100)
	out2 := frameWithSample(-1300)

	type roomCapture struct {
		start roomMessage
		media roomMessage
		query url.Values
	}
	roomSeen := make(chan roomCapture, 1)
	roomServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("room upgrade: %v", err)
			return
		}
		defer conn.Close()
		var start roomMessage
		if err := readJSON(conn, time.Second, &start); err != nil {
			t.Errorf("room start read: %v", err)
			return
		}
		var media roomMessage
		if err := readJSON(conn, time.Second, &media); err != nil {
			t.Errorf("room media read: %v", err)
			return
		}
		roomSeen <- roomCapture{start: start, media: media, query: r.URL.Query()}
		payload := append(append([]byte(nil), out1...), out2...)
		err = writeJSON(conn, time.Second, roomMessage{
			Event: "media",
			Media: &roomMedia{
				Track:   "outbound",
				Payload: base64.StdEncoding.EncodeToString(payload),
			},
		})
		if err != nil {
			t.Errorf("room write: %v", err)
		}
	}))
	defer roomServer.Close()

	evoReceived := make(chan []evolutionMessage, 1)
	evoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("token"); got != "signed" {
			t.Errorf("missing signed token, got %q", got)
		}
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("evolution upgrade: %v", err)
			return
		}
		defer conn.Close()
		err = writeJSON(conn, time.Second, evolutionMessage{
			Event:      "start",
			CallID:     "007A27AF8C472B94720D093224287272",
			SampleRate: sampleRate,
			Direction:  "incoming",
		})
		if err != nil {
			t.Errorf("evolution start write: %v", err)
			return
		}
		err = writeJSON(conn, time.Second, evolutionMessage{
			Event:      "media",
			CallID:     "007A27AF8C472B94720D093224287272",
			SampleRate: sampleRate,
			Track:      "inbound",
			Payload:    base64.StdEncoding.EncodeToString(inbound),
		})
		if err != nil {
			t.Errorf("evolution media write: %v", err)
			return
		}
		var got []evolutionMessage
		for len(got) < 2 {
			var msg evolutionMessage
			if err := readJSON(conn, 2*time.Second, &msg); err != nil {
				t.Errorf("evolution outbound read: %v", err)
				return
			}
			got = append(got, msg)
		}
		evoReceived <- got
		_ = writeJSON(conn, time.Second, evolutionMessage{Event: "stop", CallID: "007A27AF8C472B94720D093224287272"})
	}))
	defer evoServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := config{
		evolutionURL:      wsURLFromHTTP(evoServer.URL) + "/call/stream/007A27AF8C472B94720D093224287272?instance=sala2&exp=1800000000&token=signed",
		roomBridgeURL:     wsURLFromHTTP(roomServer.URL),
		dir:               "pt2ru",
		roomPrefix:        "wa-call-",
		lockDir:           t.TempDir(),
		readTimeout:       2 * time.Second,
		writeTimeout:      time.Second,
		pingInterval:      0,
		vadThreshold:      300,
		prespeechFrames:   0,
		maxQueuedFrames:   4,
		maxRoomReconnects: 0,
	}
	err := run(ctx, cfg)
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		t.Fatalf("adapter run failed: %v", err)
	}

	var room roomCapture
	select {
	case room = <-roomSeen:
	case <-time.After(time.Second):
		t.Fatal("room bridge did not receive mapped media")
	}
	if room.query.Get("room") != "wa-call-007A27AF8C472B94720D093224287272" || room.query.Get("dir") != "pt2ru" {
		t.Fatalf("bad room query: %v", room.query)
	}
	if room.start.Event != "start" || room.start.Start == nil || room.start.Start.MediaFormat.SampleRate != sampleRate {
		t.Fatalf("bad room start: %#v", room.start)
	}
	if room.media.Event != "media" || room.media.Media == nil || room.media.Media.Track != "inbound" {
		t.Fatalf("bad room media envelope: %#v", room.media)
	}
	roomRaw, err := base64.StdEncoding.DecodeString(room.media.Media.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(roomRaw) != string(inbound) {
		t.Fatal("Evolution inbound frame was not preserved exactly into room_bridge media payload")
	}

	var outbound []evolutionMessage
	select {
	case outbound = <-evoReceived:
	case <-time.After(time.Second):
		t.Fatal("Evolution fake did not receive room outbound frames")
	}
	if len(outbound) != 2 {
		t.Fatalf("outbound frames = %d, want 2", len(outbound))
	}
	for i, msg := range outbound {
		if msg.Event != "media" || msg.Track != "outbound" || msg.SampleRate != sampleRate || msg.CallID != "007A27AF8C472B94720D093224287272" {
			t.Fatalf("bad outbound[%d]: %#v", i, msg)
		}
		raw, err := base64.StdEncoding.DecodeString(msg.Payload)
		if err != nil {
			t.Fatal(err)
		}
		want := out1
		if i == 1 {
			want = out2
		}
		if string(raw) != string(want) {
			t.Fatalf("outbound[%d] PCM was not preserved exactly", i)
		}
	}
}

func TestAdapterLifecycleEventsDoNotStartRoomBeforeAudio(t *testing.T) {
	inbound := frameWithSample(1400)

	type roomCapture struct {
		start roomMessage
		media roomMessage
		stop  roomMessage
	}
	roomSeen := make(chan roomCapture, 1)
	roomConnected := make(chan struct{}, 1)
	roomServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		roomConnected <- struct{}{}
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("room upgrade: %v", err)
			return
		}
		defer conn.Close()
		var start roomMessage
		if err := readJSON(conn, time.Second, &start); err != nil {
			t.Errorf("room start read: %v", err)
			return
		}
		var media roomMessage
		if err := readJSON(conn, time.Second, &media); err != nil {
			t.Errorf("room media read: %v", err)
			return
		}
		var stop roomMessage
		if err := readJSON(conn, time.Second, &stop); err != nil {
			t.Errorf("room stop read: %v", err)
			return
		}
		roomSeen <- roomCapture{start: start, media: media, stop: stop}
	}))
	defer roomServer.Close()

	evoLifecycleSent := make(chan struct{})
	evoContinue := make(chan struct{})
	evoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("evolution upgrade: %v", err)
			return
		}
		defer conn.Close()
		_ = writeJSON(conn, time.Second, evolutionMessage{Event: "start", CallID: "lifecycle-call", SampleRate: sampleRate, Direction: "incoming"})
		_ = writeJSON(conn, time.Second, evolutionMessage{Event: "state", CallID: "lifecycle-call", Phase: "active", Direction: "incoming"})
		_ = writeJSON(conn, time.Second, evolutionMessage{Event: "media_ready", CallID: "lifecycle-call", Direction: "incoming"})
		close(evoLifecycleSent)
		<-evoContinue
		_ = writeJSON(conn, time.Second, evolutionMessage{
			Event:      "media",
			CallID:     "lifecycle-call",
			SampleRate: sampleRate,
			Track:      "inbound",
			Payload:    base64.StdEncoding.EncodeToString(inbound),
		})
		_ = writeJSON(conn, time.Second, evolutionMessage{Event: "stop", CallID: "lifecycle-call"})
	}))
	defer evoServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errs := make(chan error, 1)
	go func() {
		errs <- run(ctx, config{
			evolutionURL:      wsURLFromHTTP(evoServer.URL) + "/call/stream/lifecycle-call?instance=sala2&exp=1800000000&token=signed",
			roomBridgeURL:     wsURLFromHTTP(roomServer.URL),
			dir:               "pt2ru",
			roomPrefix:        "wa-call-",
			lockDir:           t.TempDir(),
			readTimeout:       2 * time.Second,
			writeTimeout:      time.Second,
			pingInterval:      0,
			vadThreshold:      300,
			prespeechFrames:   0,
			maxQueuedFrames:   2,
			maxRoomReconnects: 0,
		})
	}()

	select {
	case <-evoLifecycleSent:
	case <-time.After(time.Second):
		t.Fatal("Evolution fake did not send lifecycle messages")
	}
	select {
	case <-roomConnected:
		t.Fatal("room bridge connected before inbound audio")
	case <-time.After(150 * time.Millisecond):
	}
	close(evoContinue)

	var room roomCapture
	select {
	case room = <-roomSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("room bridge did not receive audio lifecycle")
	}
	if room.start.Event != "start" || room.start.Start == nil || room.start.Start.CallSid != "lifecycle-call" {
		t.Fatalf("bad room start: %#v", room.start)
	}
	if room.media.Event != "media" || room.media.Media == nil || room.media.Media.Track != "inbound" {
		t.Fatalf("bad room media: %#v", room.media)
	}
	if room.stop.Event != "stop" {
		t.Fatalf("bad room stop: %#v", room.stop)
	}
	select {
	case err := <-errs:
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			t.Fatalf("adapter run failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not stop after Evolution stop")
	}
}

func TestAdapterDoesNotConnectRoomBridgeForSilenceOnly(t *testing.T) {
	var roomConnections atomicCounter
	roomServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		roomConnections.Add(1)
		t.Errorf("room bridge should not be connected for silence-only input")
	}))
	defer roomServer.Close()

	evoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("evolution upgrade: %v", err)
			return
		}
		defer conn.Close()
		_ = writeJSON(conn, time.Second, evolutionMessage{
			Event:      "media",
			CallID:     "silent-call",
			SampleRate: sampleRate,
			Track:      "inbound",
			Payload:    base64.StdEncoding.EncodeToString(frameWithSample(0)),
		})
		_ = writeJSON(conn, time.Second, evolutionMessage{Event: "stop", CallID: "silent-call"})
	}))
	defer evoServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := run(ctx, config{
		evolutionURL:      wsURLFromHTTP(evoServer.URL) + "/call/stream/silent-call?instance=sala2&exp=1800000000&token=signed",
		roomBridgeURL:     wsURLFromHTTP(roomServer.URL),
		dir:               "pt2ru",
		roomPrefix:        "wa-call-",
		lockDir:           t.TempDir(),
		readTimeout:       time.Second,
		writeTimeout:      time.Second,
		vadThreshold:      300,
		prespeechFrames:   1,
		maxQueuedFrames:   2,
		maxRoomReconnects: 0,
	})
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		t.Fatalf("adapter run failed: %v", err)
	}
	if roomConnections.Load() != 0 {
		t.Fatalf("room bridge connections = %d, want 0", roomConnections.Load())
	}
}

func TestAcquireCallLockPreventsDuplicateOwner(t *testing.T) {
	lockPath := t.TempDir() + "/call.lock"
	unlock, err := acquireCallLock(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireCallLock(lockPath); err == nil || !strings.Contains(err.Error(), "another local adapter") {
		t.Fatalf("expected duplicate owner error, got %v", err)
	}
	unlock()
	if unlock2, err := acquireCallLock(lockPath); err != nil {
		t.Fatalf("lock should be reusable after unlock: %v", err)
	} else {
		unlock2()
	}
}

type atomicCounter struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCounter) Add(v int) {
	c.mu.Lock()
	c.n += v
	c.mu.Unlock()
}

func (c *atomicCounter) Load() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func frameWithSample(sample int16) []byte {
	frame := make([]byte, frameBytes)
	for i := 0; i < frameSamples; i++ {
		binary.LittleEndian.PutUint16(frame[i*2:], uint16(sample))
	}
	return frame
}

func writeJSON(conn *websocket.Conn, timeout time.Duration, v any) error {
	_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	return conn.WriteJSON(v)
}

func readJSON(conn *websocket.Conn, timeout time.Duration, v any) error {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	return conn.ReadJSON(v)
}

func wsURLFromHTTP(serverURL string) string {
	return "ws" + strings.TrimPrefix(serverURL, "http")
}
