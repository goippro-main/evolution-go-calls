package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadStreamJSONLAcceptsInboundPCM16Frames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stream.jsonl")
	frame := make([]byte, frameBytes)
	for i := 0; i < frameSamples; i++ {
		binary.LittleEndian.PutUint16(frame[i*2:], uint16(int16(100+i%7)))
	}
	lines := []streamMessage{
		{Event: "start", CallID: "call-1", SampleRate: sampleRate, Direction: "incoming"},
		{Event: "media", CallID: "call-1", SampleRate: sampleRate, Track: "inbound", Payload: base64.StdEncoding.EncodeToString(frame)},
		{Event: "media", CallID: "call-1", SampleRate: sampleRate, Track: "outbound", Payload: base64.StdEncoding.EncodeToString(frame)},
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(file)
	for _, line := range lines {
		if err := enc.Encode(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	samples, stats, meta, err := readStreamJSONL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != frameSamples {
		t.Fatalf("samples = %d, want %d", len(samples), frameSamples)
	}
	if stats.NonZeroSamples != frameSamples || !stats.FrameAligned {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if meta.CallID != "call-1" || meta.Direction != "incoming" {
		t.Fatalf("unexpected metadata: %#v", meta)
	}
}

func TestWriteOutboundJSONLProduces960SampleOutboundFrames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbound.jsonl")
	samples := make([]int16, frameSamples+5)
	for i := range samples {
		samples[i] = int16(i + 1)
	}
	if err := writeOutboundJSONL(path, "call-2", samples); err != nil {
		t.Fatal(err)
	}
	file := mustOpen(t, path)
	scanner := bufio.NewScanner(file)
	frames := 0
	for scanner.Scan() {
		frames++
		var msg streamMessage
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			t.Fatal(err)
		}
		raw, err := base64.StdEncoding.DecodeString(msg.Payload)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Event != "media" || msg.Track != "outbound" || msg.SampleRate != sampleRate || len(raw) != frameBytes {
			t.Fatalf("bad frame: %#v bytes=%d", msg, len(raw))
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if frames != 2 {
		t.Fatalf("frames = %d, want 2", frames)
	}
}

func TestValidateAIHTTPConfigRequiresExplicitLocalNetworkAndBlocks198(t *testing.T) {
	base := config{agentProvider: "ai-room-http", aiRoomSiteKey: "one-card"}
	if err := validateAIHTTPConfig(base); err == nil || !strings.Contains(err.Error(), "-allow-network") {
		t.Fatalf("expected allow-network error, got %v", err)
	}
	local := base
	local.allowNetwork = true
	local.aiRoomURL = "http://127.0.0.1:8092"
	if err := validateAIHTTPConfig(local); err != nil {
		t.Fatalf("local URL should be allowed: %v", err)
	}
	blocked := local
	blocked.aiRoomURL = "http://81.17.140.198:8092"
	blocked.allowAIHost = "81.17.140.198"
	if err := validateAIHTTPConfig(blocked); err == nil || !strings.Contains(err.Error(), "blocked") {
		t.Fatalf(".198 should stay blocked, got %v", err)
	}
	remote := local
	remote.aiRoomURL = "http://203.0.113.10:8092"
	if err := validateAIHTTPConfig(remote); err == nil || !strings.Contains(err.Error(), "not loopback") {
		t.Fatalf("remote host should require allowlist, got %v", err)
	}
	remote.allowAIHost = "203.0.113.10"
	if err := validateAIHTTPConfig(remote); err != nil {
		t.Fatalf("explicit lab host allowlist should pass: %v", err)
	}
}

func TestOfflineRunWithMocksWritesPrivateArtifacts(t *testing.T) {
	dir := t.TempDir()
	in := filepath.Join(dir, "in.wav")
	samples := make([]int16, frameSamples+11)
	for i := range samples {
		samples[i] = int16(1000 - i%25)
	}
	if err := writePCM16MonoWAV(in, padSamples(samples, frameSamples)); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	err := run(context.Background(), config{
		inWAV:           in,
		outDir:          out,
		sttProvider:     "fixed",
		fixedTranscript: "Olá, teste offline.",
		agentProvider:   "mock",
		mockReply:       "Resposta offline.",
		ttsProvider:     "tone",
		aiRoomLanguage:  "pt",
		maxReplyChars:   80,
		sttTimeout:      1,
		agentTimeout:    1,
		ttsTimeout:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"bridge.jsonl",
		"inbound-normalized-16k-mono-pcm16.wav",
		"transcript.txt",
		"reply.txt",
		"response-16k-mono-pcm16-960frames.wav",
		"outbound-media.jsonl",
		"run.json",
	} {
		info, err := os.Stat(filepath.Join(out, name))
		if err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("%s mode = %v, want 0600", name, info.Mode().Perm())
		}
	}
	respSamples, stats, err := readPCM16MonoWAV(filepath.Join(out, "response-16k-mono-pcm16-960frames.wav"))
	if err != nil {
		t.Fatal(err)
	}
	if len(respSamples) == 0 || !stats.FrameAligned || stats.NonZeroSamples == 0 {
		t.Fatalf("bad response audio: len=%d stats=%#v", len(respSamples), stats)
	}
	rawLog, err := os.ReadFile(filepath.Join(out, "bridge.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(rawLog), "Olá, teste offline.") || strings.Contains(string(rawLog), "Resposta offline.") {
		t.Fatalf("log leaked text with logText disabled: %s", rawLog)
	}
}

func TestReadPCM16MonoWAVRejectsWrongRate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.wav")
	var raw []byte
	raw = append(raw, []byte("RIFF")...)
	raw = binary.LittleEndian.AppendUint32(raw, 38)
	raw = append(raw, []byte("WAVEfmt ")...)
	raw = binary.LittleEndian.AppendUint32(raw, 16)
	raw = binary.LittleEndian.AppendUint16(raw, 1)
	raw = binary.LittleEndian.AppendUint16(raw, 1)
	raw = binary.LittleEndian.AppendUint32(raw, 8000)
	raw = binary.LittleEndian.AppendUint32(raw, 16000)
	raw = binary.LittleEndian.AppendUint16(raw, 2)
	raw = binary.LittleEndian.AppendUint16(raw, 16)
	raw = append(raw, []byte("data")...)
	raw = binary.LittleEndian.AppendUint32(raw, 2)
	raw = binary.LittleEndian.AppendUint16(raw, 0)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readPCM16MonoWAV(path); err == nil || !strings.Contains(err.Error(), "16000 Hz") {
		t.Fatalf("expected rate error, got %v", err)
	}
}

func mustOpen(t *testing.T, path string) *os.File {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	return file
}
