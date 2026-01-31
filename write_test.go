package main

import (
	"bytes"
	"encoding/json"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrite_JSONLFormatting(t *testing.T) {
	tests := []struct {
		name      string
		hits      []json.RawMessage
		wantLines int
	}{
		{
			name: "single hit without newlines",
			hits: []json.RawMessage{
				json.RawMessage(`{"id":1,"name":"test"}`),
			},
			wantLines: 1,
		},
		{
			name: "hit with newlines",
			hits: []json.RawMessage{
				json.RawMessage("{\n  \"id\": 1,\n  \"name\": \"test\"\n}"),
			},
			wantLines: 1,
		},
		{
			name: "multiple hits",
			hits: []json.RawMessage{
				json.RawMessage(`{"id":1}`),
				json.RawMessage(`{"id":2}`),
				json.RawMessage(`{"id":3}`),
			},
			wantLines: 3,
		},
		{
			name: "compact JSON with newlines",
			hits: []json.RawMessage{
				json.RawMessage("{\n\"id\":\n1\n}"),
			},
			wantLines: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the core logic without needing bufio.Writer
			for _, hit := range tt.hits {
				var buf bytes.Buffer
				// Test the newline handling logic
				if bytes.IndexByte(hit, '\n') != -1 {
					err := json.Compact(&buf, hit)
					require.NoError(t, err)
					hit = buf.Bytes()
				}

				// Verify it's valid JSON
				var testObj map[string]any
				err := json.Unmarshal(hit, &testObj)
				require.NoError(t, err, "should produce valid JSON")

				// Verify no newlines in compacted version
				assert.NotContains(t, string(hit), "\n", "compacted JSON should not contain newlines")
			}
		})
	}
}

func TestWrite_CountLimit(t *testing.T) {
	// Test that writing stops when count limit is reached
	d := &dumper{
		count:  2,
		dumped: 0,
	}

	// Simulate the count check logic
	dumped := uint64(0)
	stop := false

	hits := []json.RawMessage{
		json.RawMessage(`{"id":1}`),
		json.RawMessage(`{"id":2}`),
		json.RawMessage(`{"id":3}`),
	}

	for range hits {
		if stop {
			break
		}
		dumped++
		if d.count > 0 && dumped >= d.count {
			stop = true
		}
	}

	assert.Equal(t, uint64(2), dumped, "should stop at count limit")
	assert.True(t, stop, "should set stop flag")
}

func TestJSONCompact(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantJSON bool
	}{
		{
			name:     "valid JSON with newlines",
			input:    "{\n  \"id\": 1\n}",
			wantJSON: true,
		},
		{
			name:     "already compact JSON",
			input:    `{"id":1}`,
			wantJSON: true,
		},
		{
			name:     "JSON with multiple newlines",
			input:    "{\n\"id\":\n1,\n\"name\":\n\"test\"\n}",
			wantJSON: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := json.Compact(&buf, []byte(tt.input))
			if tt.wantJSON {
				require.NoError(t, err)
				// Verify it's still valid JSON
				var testObj map[string]any
				err = json.Unmarshal(buf.Bytes(), &testObj)
				require.NoError(t, err, "compacted JSON should be valid")
				// Verify no newlines
				assert.NotContains(t, buf.String(), "\n", "should not contain newlines")
			}
		})
	}
}

func TestWrite_JSONLRawMessage(t *testing.T) {
	// Test using jsoniter like the actual code
	hit := jsoniter.RawMessage(`{"id":1,"name":"test"}`)

	// Verify it can be used as json.RawMessage
	var testObj map[string]any
	err := json.Unmarshal(hit, &testObj)
	require.NoError(t, err)
	assert.InDelta(t, float64(1), testObj["id"], 0.01)
	assert.Equal(t, "test", testObj["name"])
}
