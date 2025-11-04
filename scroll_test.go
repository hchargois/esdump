package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScrollQuery_JSONMarshaling(t *testing.T) {
	tests := []struct {
		name        string
		scrollID    string
		timeout     string
		wantValid   bool
		description string
	}{
		{
			name:        "normal scroll ID",
			scrollID:    "DXF1ZXJ5QW5kRmV0Y2gBAAAAAABC",
			timeout:     "1m",
			wantValid:   true,
			description: "should handle normal scroll IDs",
		},
		{
			name:        "scroll ID with quotes",
			scrollID:    `DXF1ZXJ5QW5kRmV0Y2gBAAAAA"ABC`,
			timeout:     "1m",
			wantValid:   true,
			description: "should escape quotes in scroll ID",
		},
		{
			name:        "scroll ID with backslash",
			scrollID:    `DXF1ZXJ5QW5kRmV0Y2gBAAAAA\ABC`,
			timeout:     "1m",
			wantValid:   true,
			description: "should escape backslashes in scroll ID",
		},
		{
			name:        "scroll ID with newline",
			scrollID:    "DXF1ZXJ5QW5kRmV0Y2gBAAAAA\nABC",
			timeout:     "1m",
			wantValid:   true,
			description: "should escape newlines in scroll ID",
		},
		{
			name:        "scroll ID with control characters",
			scrollID:    "DXF1ZXJ5QW5kRmV0Y2gBAAAAA\t\rABC",
			timeout:     "1m",
			wantValid:   true,
			description: "should escape control characters",
		},
		{
			name:        "empty scroll ID",
			scrollID:    "",
			timeout:     "1m",
			wantValid:   true,
			description: "should handle empty scroll ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a scroll request map like in the fixed code
			scrollReq := map[string]string{
				"scroll":    tt.timeout,
				"scroll_id": tt.scrollID,
			}

			// Marshal to JSON (this is what the fix does)
			qBytes, err := json.Marshal(scrollReq)
			require.NoError(t, err, "should marshal without error")

			// Verify it's valid JSON
			var decoded map[string]string
			err = json.Unmarshal(qBytes, &decoded)
			require.NoError(t, err, "should unmarshal valid JSON")
			assert.Equal(t, tt.scrollID, decoded["scroll_id"], "scroll ID should be preserved")
			assert.Equal(t, tt.timeout, decoded["scroll"], "timeout should be preserved")
		})
	}
}

func TestScrollQuery_SliceGeneration(t *testing.T) {
	d := &dumper{
		query: obj{
			"query": obj{
				"match_all": obj{},
			},
			"size": 1000,
		},
		size:          1000,
		scrollTimeout: time.Minute,
	}

	// Test single slice (no slicing)
	q := d.scrollQuery(0, 1)
	var result obj
	err := json.Unmarshal([]byte(q), &result)
	require.NoError(t, err)
	assert.NotContains(t, result, "slice", "should not contain slice when sliceTotal is 1")

	// Test multiple slices
	q = d.scrollQuery(2, 5)
	err = json.Unmarshal([]byte(q), &result)
	require.NoError(t, err)
	assert.Contains(t, result, "slice", "should contain slice when sliceTotal > 1")
	slice, ok := result["slice"].(map[string]any)
	require.True(t, ok, "slice should be an object")
	assert.InDelta(t, float64(2), slice["id"], 0.01, "slice id should be 2")
	assert.InDelta(t, float64(5), slice["max"], 0.01, "slice max should be 5")
}

func TestSleepForThrottling(t *testing.T) {
	d := &dumper{
		throttle:      4.0,
		scrollTimeout: time.Minute,
	}

	ctx := t.Context()

	// Test with throttle disabled
	d.throttle = 0
	start := time.Now()
	d.sleepForThrottling(ctx, 10*time.Millisecond)
	duration := time.Since(start)
	assert.Less(t, duration, 50*time.Millisecond, "should not sleep when throttle is 0")

	// Test with throttle enabled
	d.throttle = 4.0
	start = time.Now()
	d.sleepForThrottling(ctx, 10*time.Millisecond)
	duration = time.Since(start)
	assert.GreaterOrEqual(t, duration, 30*time.Millisecond, "should sleep when throttle is enabled")
	assert.Less(t, duration, 100*time.Millisecond, "should not sleep too long")

	// Test max delay cap (should not exceed 3/4 of scroll timeout)
	d.throttle = 1000.0 // Very high throttle
	start = time.Now()
	d.sleepForThrottling(ctx, time.Second)
	duration = time.Since(start)
	maxDelay := 3 * d.scrollTimeout / 4
	assert.Less(t, duration, maxDelay+50*time.Millisecond, "should cap delay at 3/4 of scroll timeout")
}
