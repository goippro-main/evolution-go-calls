package call_stream

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

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
	Phase      string `json:"phase,omitempty"`
	Direction  string `json:"direction,omitempty"`
	Stats      any    `json:"stats,omitempty"`
}

type bridge struct {
	conn                   *websocket.Conn
	writeMu                sync.Mutex
	incoming               chan []float32
	closed                 chan struct{}
	closeOnce              sync.Once
	inboundReady           atomic.Bool
	outboundReady          atomic.Bool
	silence                []float32
	inboundFrames          atomic.Int64
	inboundNonZeroFrames   atomic.Int64
	inboundNonZeroSamples  atomic.Int64
	inboundPeak            atomic.Int64
	outboundQueuedFrames   atomic.Int64
	outboundFrames         atomic.Int64
	outboundNonZeroFrames  atomic.Int64
	outboundNonZeroSamples atomic.Int64
	outboundPeak           atomic.Int64
	droppedOutboundFrames  atomic.Int64
	firstInboundUnixNano   atomic.Int64
	lastInboundUnixNano    atomic.Int64
	firstOutboundUnixNano  atomic.Int64
	lastOutboundUnixNano   atomic.Int64
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

func (b *bridge) allowOutbound() {
	b.outboundReady.Store(true)
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

func (b *bridge) writeState(callID, phase, direction string) error {
	return b.writeJSON(wsMessage{Event: "state", CallID: callID, Phase: phase, Direction: direction})
}

func (b *bridge) diagnostics() map[string]any {
	return map[string]any{
		"inboundFrames":          b.inboundFrames.Load(),
		"inboundNonZeroFrames":   b.inboundNonZeroFrames.Load(),
		"inboundNonZeroSamples":  b.inboundNonZeroSamples.Load(),
		"inboundPeak":            b.inboundPeak.Load(),
		"outboundQueuedFrames":   b.outboundQueuedFrames.Load(),
		"outboundFrames":         b.outboundFrames.Load(),
		"outboundNonZeroFrames":  b.outboundNonZeroFrames.Load(),
		"outboundNonZeroSamples": b.outboundNonZeroSamples.Load(),
		"outboundPeak":           b.outboundPeak.Load(),
		"droppedOutboundFrames":  b.droppedOutboundFrames.Load(),
		"firstInboundTs":         unixNanoTime(b.firstInboundUnixNano.Load()),
		"lastInboundTs":          unixNanoTime(b.lastInboundUnixNano.Load()),
		"firstOutboundTs":        unixNanoTime(b.firstOutboundUnixNano.Load()),
		"lastOutboundTs":         unixNanoTime(b.lastOutboundUnixNano.Load()),
		"bidirectionalMedia":     b.inboundNonZeroFrames.Load() > 0 && b.outboundNonZeroFrames.Load() > 0,
	}
}

func unixNanoTime(value int64) any {
	if value == 0 {
		return nil
	}
	return time.Unix(0, value).UTC()
}

func (b *bridge) recordInbound(frame []float32) {
	nonZeroFrames, nonZeroSamples, peak := frameStats(frame)
	b.inboundFrames.Add(1)
	b.inboundNonZeroFrames.Add(nonZeroFrames)
	b.inboundNonZeroSamples.Add(nonZeroSamples)
	maxAtomic(&b.inboundPeak, peak)
	now := time.Now().UTC().UnixNano()
	b.firstInboundUnixNano.CompareAndSwap(0, now)
	b.lastInboundUnixNano.Store(now)
}

func (b *bridge) recordOutbound(frame []float32) {
	nonZeroFrames, nonZeroSamples, peak := frameStats(frame)
	b.outboundFrames.Add(1)
	b.outboundNonZeroFrames.Add(nonZeroFrames)
	b.outboundNonZeroSamples.Add(nonZeroSamples)
	maxAtomic(&b.outboundPeak, peak)
	now := time.Now().UTC().UnixNano()
	b.firstOutboundUnixNano.CompareAndSwap(0, now)
	b.lastOutboundUnixNano.Store(now)
}

func maxAtomic(target *atomic.Int64, value int64) {
	for current := target.Load(); value > current; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func frameStats(frame []float32) (nonZeroFrames, nonZeroSamples, peak int64) {
	for _, sample := range frame {
		if sample < 0 {
			sample = -sample
		}
		if sample > 0.00025 {
			nonZeroSamples++
		}
		scaled := int64(sample * 32767)
		if scaled > peak {
			peak = scaled
		}
	}
	if nonZeroSamples > 0 {
		nonZeroFrames = 1
	}
	return nonZeroFrames, nonZeroSamples, peak
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
	if err := b.writeJSON(wsMessage{Event: "media", Track: "inbound", Payload: payload}); err != nil {
		return err
	}
	b.recordInbound(frame)
	return nil
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
	if !b.outboundReady.Load() {
		return b.silence, nil
	}
	select {
	case frame, ok := <-b.incoming:
		if !ok {
			return nil, io.EOF
		}
		b.recordOutbound(frame)
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
			_ = b.writeJSON(wsMessage{Event: "diagnostics", Stats: b.diagnostics()})
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
		if !b.outboundReady.Load() {
			continue
		}
		select {
		case b.incoming <- frame:
			b.outboundQueuedFrames.Add(1)
		case <-b.closed:
			return io.EOF
		default:
			b.droppedOutboundFrames.Add(1)
			// Drop newest input under backpressure instead of blocking the websocket
			// reader and starving call teardown/control messages.
		}
	}
}
