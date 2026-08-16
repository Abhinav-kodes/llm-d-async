package asyncworker

import "testing"

func TestParseUsage(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantInput int64
		wantOut   int64
		wantOK    bool
	}{
		{name: "both tokens", body: `{"usage":{"prompt_tokens":25,"completion_tokens":13}}`, wantInput: 25, wantOut: 13, wantOK: true},
		{name: "prompt only", body: `{"usage":{"prompt_tokens":7}}`, wantInput: 7, wantOut: 0, wantOK: true},
		{name: "completion only", body: `{"usage":{"completion_tokens":9}}`, wantInput: 0, wantOut: 9, wantOK: true},
		{name: "no usage", body: `{"choices":[{"text":"hi"}]}`, wantOK: false},
		{name: "usage null", body: `{"usage":null}`, wantOK: false},
		{name: "non-json", body: `not json`, wantOK: false},
		{name: "empty", body: ``, wantOK: false},
		{name: "negative clamped", body: `{"usage":{"prompt_tokens":-5,"completion_tokens":3}}`, wantInput: 0, wantOut: 3, wantOK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input, output, ok := ParseUsage([]byte(tt.body))
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if input != tt.wantInput || output != tt.wantOut {
				t.Errorf("ParseUsage = (%d, %d), want (%d, %d)", input, output, tt.wantInput, tt.wantOut)
			}
		})
	}
}
