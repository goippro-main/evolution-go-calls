package call_stream

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/gorilla/websocket"
	"github.com/purpshell/meowcaller"
)

const maxQueuedAudioFrames = 50

// wsMessage is the JSON envelope used by the audio-only media bridge. Audio is
// always mono, 16 kHz, PCM16LE, with one 960-sample frame per message.
type wsMessage struct {
	Event      string `json:"event"`
	CallID     string `json:"callId,omitempty"`
	SampleRate int    `json:"sampleRate,omitempty"`
	Track      string `json:"track,omitempty"`
	Payload    string `json:"payload,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

type bridge struct {
	conn         *websocket.Conn
	writeMu      sync.Mutex
	incoming     chan []float32
	closed       chan struct{}
	closeOnce    sync.Once
	inboundReady atomic.Bool
	silence      []float32
}

func newBridge(conn *websocket.Conn) *bridge {
	return &bridge{
		conn:     conn,
		incoming: make(chan []float32, maxQueuedAudioFrames),
		closed:   make(chan struct{}),
		silence:  make([]float32, meowcaller.FrameSamples),
	}
}

func (b *bridge) allowInbound() {
	b.inboundReady.Store(true)
}

func (b *bridge) writeJSON(msg wsMessage) error {
	if b.conn == nil {
		return errors.New("call stream: websocket is nil")
	}
	b.writeMu.Lock()
	defer b.writeMu.Unlock()
	return b.conn.WriteJSON(msg)
}

func (b *bridge) writeStart(callID string) error {
	return b.writeJSON(wsMessage{
		Event:      "start",
		CallID:     callID,
		SampleRate: meowcaller.SampleRate,
	})
}

// WriteFrame implements meowcaller.AudioSink. Frames received before the call
// becomes active are deliberately discarded; they must never be exposed as live
// media to an operator client.
func (b *bridge) WriteFrame(frame []float32) error {
	if !b.inboundReady.Load() {
		return nil
	}
	if len(frame) != meowcaller.FrameSamples {
		return fmt.Errorf("call stream: inbound frame has %d samples, want %d", len(frame), meowcaller.FrameSamples)
	}
	payload := base64.StdEncoding.EncodeToString(pcm16FromFloat32(frame))
	return b.writeJSON(wsMessage{Event: "media", Track: "inbound", Payload: payload})
}

// ReadFrame implements meowcaller.AudioSource. It is intentionally
// non-blocking: meowcaller must keep its relay send loop running with silence
// until the call is active and whenever the operator has no queued audio.
func (b *bridge) ReadFrame() ([]float32, error) {
	select {
	case <-b.closed:
		return nil, io.EOF
	default:
	}
	if !b.inboundReady.Load() {
		return b.silence, nil
	}
	select {
	case frame, ok := <-b.incoming:
		if !ok {
			return nil, io.EOF
		}
		return frame, nil
	case <-b.closed:
		return nil, io.EOF
	default:
		return b.silence, nil
	}
}

// Close satisfies AudioSink and AudioSource. It is safe for callbacks, the
// websocket reader, and the HTTP handler to call concurrently.
func (b *bridge) Close() error {
	b.closeOnce.Do(func() {
		if b.conn != nil {
			_ = b.writeJSON(wsMessage{Event: "stop", Reason: "hangup"})
		}
		close(b.closed)
		if b.conn != nil {
			_ = b.conn.Close()
		}
	})
	return nil
}

func decodeOutboundAudio(msg wsMessage) ([]float32, error) {
	if msg.SampleRate != 0 && msg.SampleRate != meowcaller.SampleRate {
		return nil, fmt.Errorf("call stream: unsupported sample rate %d", msg.SampleRate)
	}
	if msg.Payload == "" {
		return nil, errors.New("call stream: media payload is empty")
	}
	raw, err := base64.StdEncoding.DecodeString(msg.Payload)
	if err != nil {
		return nil, fmt.Errorf("call stream: invalid base64 payload: %w", err)
	}
	wantBytes := meowcaller.FrameSamples * 2
	if len(raw) != wantBytes {
		return nil, fmt.Errorf("call stream: media payload has %d PCM bytes, want %d", len(raw), wantBytes)
	}
	return float32FromPCM16(raw), nil
}

// readLoop is the sole websocket reader. Writers use writeJSON's mutex, so
// meowcaller's receive callbacks and this goroutine never write concurrently.
func (b *bridge) readLoop() error {
	for {
		var msg wsMessage
		if err := b.conn.ReadJSON(&msg); err != nil {
			b.Close()
			return err
		}
		switch {
		case msg.Event == "stop":
			b.Close()
			return nil
		case msg.Event != "media" || msg.Track != "outbound":
			// Unknown events are ignored for forwards compatibility.
			continue
		}

		frame, err := decodeOutboundAudio(msg)
		if err != nil {
			b.Close()
			return err
		}
		if !b.inboundReady.Load() {
			continue
		}
		select {
		case b.incoming <- frame:
		case <-b.closed:
			return io.EOF
		default:
			// Drop newest input under backpressure instead of blocking the websocket
			// reader and starving call teardown/control messages.
		}
	}
}
