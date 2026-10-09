package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPadSamplesAlignsTo960(t *testing.T) {
	samples := []int16{1, 2, 3}
	got := padSamples(samples, frameSamples)
	if len(got) != frameSamples {
		t.Fatalf("len = %d, want %d", len(got), frameSamples)
	}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("prefix changed: %#v", got[:3])
	}
	if got[len(got)-1] != 0 {
		t.Fatal("padding must be silence")
	}
}

func TestWriteReadPCM16MonoWAVAndStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "voice.wav")
	samples := make([]int16, frameSamples+17)
	for i := range samples {
		samples[i] = int16(1000 - i%20)
	}
	padded := padSamples(samples, frameSamples)
	if err := writePCM16MonoWAV(path, padded); err != nil {
		t.Fatal(err)
	}
	got, stats, err := readPCM16MonoWAV(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != frameSamples*2 {
		t.Fatalf("samples = %d, want %d", len(got), frameSamples*2)
	}
	if !stats.FrameAligned || stats.Frames960 != 2 {
		t.Fatalf("unexpected frame stats: %#v", stats)
	}
	if stats.SampleRate != sampleRate || stats.Channels != 1 || stats.BitsPerSample != 16 {
		t.Fatalf("unexpected wav stats: %#v", stats)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode = %v, want 0600", info.Mode().Perm())
	}
}

func TestStripANSIAndCleanSpeechText(t *testing.T) {
	raw := "\x1b[?25l\x1b[1G  Olá!\x1b[0m\noutput_txt: saving output\n\nTudo bem.\x00"
	got := cleanSpeechText(raw)
	if got != "Olá! Tudo bem." {
		t.Fatalf("cleaned = %q", got)
	}
}

func TestBuildPromptIncludesDisclosureInstructionAndTranscript(t *testing.T) {
	prompt := buildPrompt("Reply as AI.", "Olá mundo")
	if !strings.Contains(prompt, "Reply as AI.") || !strings.Contains(prompt, "Olá mundo") {
		t.Fatalf("prompt missing expected content: %q", prompt)
	}
}

func TestResolveVoiceUsesCyrillicAwareDefault(t *testing.T) {
	if got := resolveVoice("auto", "Здравствуйте"); got != "Milena" {
		t.Fatalf("voice = %q, want Milena", got)
	}
	if got := resolveVoice("auto", "Olá mundo"); got != "Joana" {
		t.Fatalf("voice = %q, want Joana", got)
	}
	if got := resolveVoice("Daniel", "Здравствуйте"); got != "Daniel" {
		t.Fatalf("explicit voice ignored: %q", got)
	}
}

func TestLoggerDoesNotIncludeTextUnlessEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipeline.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	logger := &eventLogger{enc: json.NewEncoder(file)}
	logger.write("stt_completed", textFields(false, "secret transcript", map[string]any{"chars": 17}))
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secret transcript") || strings.Contains(string(raw), "text") {
		t.Fatalf("sensitive text leaked into log: %s", raw)
	}
}

func TestLoggerCanIncludeTextWhenExplicitlyEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipeline.jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	logger := &eventLogger{enc: json.NewEncoder(file)}
	logger.write("stt_completed", textFields(true, "visible transcript", map[string]any{"chars": 18}))
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(mustOpen(t, path))
	if !scanner.Scan() {
		t.Fatal("missing log line")
	}
	if !strings.Contains(scanner.Text(), "visible transcript") {
		t.Fatalf("explicit log text missing: %s", scanner.Text())
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
