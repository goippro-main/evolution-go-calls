package meowcaller

import "testing"

type playoutTestSink struct {
	frames [][]float32
}

func (s *playoutTestSink) WriteFrame(frame []float32) error {
	clone := append([]float32(nil), frame...)
	s.frames = append(s.frames, clone)
	return nil
}

func (s *playoutTestSink) Close() error { return nil }

func TestAudioPlayoutPadsShortFrameToCodecInterval(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	first := make([]float32, FrameSamples)
	short := make([]float32, 160)
	current := make([]float32, FrameSamples)
	for i := range short {
		short[i] = 0.25
	}

	if started, err := playout.Push(0, first, sink); err != nil || started {
		t.Fatalf("first push = (%v, %v), want (false, nil)", started, err)
	}
	if started, err := playout.Push(FrameSamples, short, sink); err != nil || started {
		t.Fatalf("second push = (%v, %v), want (false, nil)", started, err)
	}
	started, err := playout.Push(3*FrameSamples, current, sink)
	if err != nil || !started {
		t.Fatalf("third push = (%v, %v), want (true, nil)", started, err)
	}
	if len(sink.frames) != 2 {
		t.Fatalf("writes = %d, want 2", len(sink.frames))
	}
	if len(sink.frames[0]) != FrameSamples || len(sink.frames[1]) != FrameSamples {
		t.Fatalf("write lengths = [%d %d], want [%d %d]", len(sink.frames[0]), len(sink.frames[1]), FrameSamples, FrameSamples)
	}
	for i, sample := range sink.frames[1] {
		want := float32(0)
		if i < len(short) {
			want = 0.25
		}
		if sample != want {
			t.Fatalf("padded frame[%d] = %f, want %f", i, sample, want)
		}
	}

	if started, err = playout.Push(4*FrameSamples, current, sink); err != nil || started {
		t.Fatalf("steady push = (%v, %v), want (false, nil)", started, err)
	}
	if len(sink.frames) != 3 || len(sink.frames[2]) != FrameSamples {
		t.Fatalf("steady writes = %v, want third %d-sample frame", frameLengths(sink.frames), FrameSamples)
	}
}

func TestAudioPlayoutDoesNotReplayElapsedGap(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	frame := make([]float32, FrameSamples)

	if _, err := playout.Push(0, frame, sink); err != nil {
		t.Fatal(err)
	}
	if _, err := playout.Push(FrameSamples, frame, sink); err != nil {
		t.Fatal(err)
	}
	started, err := playout.Push(8*FrameSamples, frame, sink)
	if err != nil || !started {
		t.Fatalf("gap push = (%v, %v), want (true, nil)", started, err)
	}
	if got := frameLengths(sink.frames); len(got) != 2 || got[0] != FrameSamples || got[1] != FrameSamples {
		t.Fatalf("write lengths = %v, want [%d %d]", got, FrameSamples, FrameSamples)
	}
}

func TestAudioPlayoutFlushPendingAfterIdleBurst(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	first := constantPCM(FrameSamples, 0.1)
	second := constantPCM(FrameSamples, 0.2)
	tail := constantPCM(FrameSamples, 0.3)

	for _, push := range []struct {
		ts    uint32
		frame []float32
	}{
		{0, first},
		{FrameSamples, second},
		{2 * FrameSamples, tail},
	} {
		if _, err := playout.Push(push.ts, push.frame, sink); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink.frames) != 2 {
		t.Fatalf("prefill writes = %d, want 2", len(sink.frames))
	}
	flushed, err := playout.FlushPending(sink)
	if err != nil || !flushed {
		t.Fatalf("flush pending = (%v, %v), want (true, nil)", flushed, err)
	}
	if len(sink.frames) != 3 {
		t.Fatalf("writes after flush = %d, want 3", len(sink.frames))
	}
	if sink.frames[2][0] != 0.3 {
		t.Fatalf("flushed frame starts with %f, want 0.3", sink.frames[2][0])
	}
	flushed, err = playout.FlushPending(sink)
	if err != nil || flushed {
		t.Fatalf("second flush pending = (%v, %v), want (false, nil)", flushed, err)
	}
	if len(sink.frames) != 3 {
		t.Fatalf("second flush wrote duplicate frame: %d", len(sink.frames))
	}
}

func TestAudioPlayoutFlushPendingBeforeStartIsNoop(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	if _, err := playout.Push(0, constantPCM(FrameSamples, 0.4), sink); err != nil {
		t.Fatal(err)
	}
	flushed, err := playout.FlushPending(sink)
	if err != nil || flushed {
		t.Fatalf("flush pending before start = (%v, %v), want (false, nil)", flushed, err)
	}
	if len(sink.frames) != 0 {
		t.Fatalf("flush before start wrote %d frames", len(sink.frames))
	}
	if playout.pending == nil {
		t.Fatal("flush before start dropped pending prefill frame")
	}
}

func TestAlignAudioFrameKeepsNaturalMultiFrameOutput(t *testing.T) {
	frame := make([]float32, 2*FrameSamples)
	aligned := alignAudioFrame(frame, 8*FrameSamples)
	if len(aligned) != len(frame) {
		t.Fatalf("aligned length = %d, want natural length %d", len(aligned), len(frame))
	}
}

func TestAudioPlayoutHandlesTimestampWrap(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	start := ^uint32(0) - uint32(FrameSamples-1)
	if _, err := playout.Push(start, make([]float32, FrameSamples), sink); err != nil {
		t.Fatal(err)
	}
	if _, err := playout.Push(0, make([]float32, FrameSamples), sink); err != nil {
		t.Fatal(err)
	}
	started, err := playout.Push(FrameSamples, make([]float32, FrameSamples), sink)
	if err != nil || !started {
		t.Fatalf("wrap push = (%v, %v), want (true, nil)", started, err)
	}
}

func TestAudioPlayoutResetsOnTimestampDiscontinuity(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	for _, timestamp := range []uint32{0, FrameSamples, 2 * FrameSamples} {
		if _, err := playout.Push(timestamp, make([]float32, FrameSamples), sink); err != nil {
			t.Fatal(err)
		}
	}
	writes := len(sink.frames)
	started, err := playout.Push(2*FrameSamples+audioPlayoutMaxGapSamples+1, make([]float32, FrameSamples), sink)
	if err != nil || started {
		t.Fatalf("discontinuity push = (%v, %v), want (false, nil)", started, err)
	}
	if playout.started || len(sink.frames) != writes {
		t.Fatal("timestamp discontinuity did not reset prefill state")
	}
}

func TestAudioPlayoutDrainHandsOffBufferedDirectFrames(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	first := constantPCM(FrameSamples, 0.2)
	second := constantPCM(FrameSamples, 0.3)
	if _, err := playout.Push(0, first, sink); err != nil {
		t.Fatal(err)
	}
	if _, err := playout.Push(FrameSamples, second, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.frames) != 0 {
		t.Fatalf("prefill unexpectedly wrote %d frames", len(sink.frames))
	}
	if err := playout.Drain(sink); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(sink.frames) != 2 || len(sink.frames[0]) != FrameSamples || len(sink.frames[1]) != FrameSamples {
		t.Fatalf("drained frame lengths = %v, want [%d %d]", frameLengths(sink.frames), FrameSamples, FrameSamples)
	}
	if sink.frames[0][0] != 0.2 || sink.frames[1][0] != 0.3 {
		t.Fatalf("drained frame starts = [%f %f], want [0.2 0.3]", sink.frames[0][0], sink.frames[1][0])
	}
	if playout.pending != nil || len(playout.prefill) != 0 || playout.prefillSamples != 0 || playout.started {
		t.Fatal("drain left direct playout state behind")
	}
}

func frameLengths(frames [][]float32) []int {
	lengths := make([]int, len(frames))
	for i, frame := range frames {
		lengths[i] = len(frame)
	}
	return lengths
}
