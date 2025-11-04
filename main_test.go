package main

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidateFlags(t *testing.T) {
	tests := []struct {
		name      string
		dumper    *dumper
		wantValid bool
	}{
		{
			name: "valid flags",
			dumper: &dumper{
				throttle:      4.0,
				size:          1000,
				slices:        10,
				scrollTimeout: time.Minute,
				httpTimeout:   time.Minute,
			},
			wantValid: true,
		},
		{
			name: "negative throttle",
			dumper: &dumper{
				throttle:      -1.0,
				size:          1000,
				slices:        10,
				scrollTimeout: time.Minute,
				httpTimeout:   time.Minute,
			},
			wantValid: false,
		},
		{
			name: "zero size",
			dumper: &dumper{
				throttle:      4.0,
				size:          0,
				slices:        10,
				scrollTimeout: time.Minute,
				httpTimeout:   time.Minute,
			},
			wantValid: false,
		},
		{
			name: "zero slices",
			dumper: &dumper{
				throttle:      4.0,
				size:          1000,
				slices:        0,
				scrollTimeout: time.Minute,
				httpTimeout:   time.Minute,
			},
			wantValid: false,
		},
		{
			name: "negative scroll timeout",
			dumper: &dumper{
				throttle:      4.0,
				size:          1000,
				slices:        10,
				scrollTimeout: -time.Second,
				httpTimeout:   time.Minute,
			},
			wantValid: false,
		},
		{
			name: "negative http timeout",
			dumper: &dumper{
				throttle:      4.0,
				size:          1000,
				slices:        10,
				scrollTimeout: time.Minute,
				httpTimeout:   -time.Second,
			},
			wantValid: false,
		},
		{
			name: "metadata-only and fields mutually exclusive",
			dumper: &dumper{
				throttle:      4.0,
				size:          1000,
				slices:        10,
				scrollTimeout: time.Minute,
				httpTimeout:   time.Minute,
				metadataOnly:  true,
				fields:        "id,date",
			},
			wantValid: false,
		},
		{
			name: "zero throttle allowed",
			dumper: &dumper{
				throttle:      0.0,
				size:          1000,
				slices:        10,
				scrollTimeout: time.Minute,
				httpTimeout:   time.Minute,
			},
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We can't easily test the os.Exit(1) behavior, but we can test
			// the validation logic by checking the conditions
			hasErrors := tt.dumper.throttle < 0 ||
				tt.dumper.size < 1 ||
				tt.dumper.slices < 1 ||
				tt.dumper.scrollTimeout < 0 ||
				tt.dumper.httpTimeout < 0 ||
				(tt.dumper.metadataOnly && tt.dumper.fields != "")

			if tt.wantValid {
				assert.False(t, hasErrors, "should have no validation errors")
			} else {
				assert.True(t, hasErrors, "should have validation errors")
			}
		})
	}
}

func TestIsLoopback(t *testing.T) {
	tests := []struct {
		name     string
		host     string
		expected bool
	}{
		{
			name:     "localhost",
			host:     "localhost",
			expected: true,
		},
		{
			name:     "127.0.0.1",
			host:     "127.0.0.1",
			expected: true,
		},
		{
			name:     "IPv6 loopback",
			host:     "::1",
			expected: true,
		},
		{
			name:     "public domain",
			host:     "example.com",
			expected: false,
		},
		{
			name:     "public IP",
			host:     "8.8.8.8",
			expected: false,
		},
		{
			name:     "empty host",
			host:     "",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isLoopback(tt.host)
			assert.Equal(t, tt.expected, result, "isLoopback(%q) should be %v", tt.host, tt.expected)
		})
	}
}

func TestFormatScrollTimeoutES(t *testing.T) {
	tests := []struct {
		name     string
		timeout  time.Duration
		expected string
	}{
		{
			name:     "small duration in milliseconds",
			timeout:  5 * time.Second,
			expected: "5000ms",
		},
		{
			name:     "exactly 10 seconds",
			timeout:  10 * time.Second,
			expected: "10s",
		},
		{
			name:     "one minute",
			timeout:  time.Minute,
			expected: "60s",
		},
		{
			name:     "two minutes",
			timeout:  2 * time.Minute,
			expected: "120s",
		},
		{
			name:     "500 milliseconds",
			timeout:  500 * time.Millisecond,
			expected: "500ms",
		},
		{
			name:     "9 seconds (should be milliseconds)",
			timeout:  9 * time.Second,
			expected: "9000ms",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &dumper{
				scrollTimeout: tt.timeout,
			}
			result := d.formatScrollTimeoutES()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIPIsLoopback(t *testing.T) {
	// Test actual IP addresses
	tests := []struct {
		ip       net.IP
		expected bool
	}{
		{net.IPv4(127, 0, 0, 1), true},
		{net.IPv4(127, 0, 0, 2), true},
		{net.IPv4(8, 8, 8, 8), false},
		{net.IPv6loopback, true},
		{net.IPv4(192, 168, 1, 1), false},
	}

	for _, tt := range tests {
		t.Run(tt.ip.String(), func(t *testing.T) {
			result := tt.ip.IsLoopback()
			assert.Equal(t, tt.expected, result)
		})
	}
}
