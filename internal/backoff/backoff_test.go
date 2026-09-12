package backoff

import (
	"testing"
	"time"
)

func TestDelayStaysWithinBounds(t *testing.T) {
	c := Config{Base: 100 * time.Millisecond, Max: 2 * time.Second}

	for attempts := 0; attempts <= 12; attempts++ {
		ceiling := c.Base << uint(attempts)
		if ceiling > c.Max || ceiling <= 0 {
			ceiling = c.Max
		}
		// Sample many times: jitter is random, so a single draw proves little.
		for i := 0; i < 200; i++ {
			d := c.Delay(attempts)
			if d < 0 || d >= ceiling {
				t.Fatalf("attempts=%d: delay %v outside [0, %v)", attempts, d, ceiling)
			}
		}
	}
}

func TestDelayIsCappedAtMax(t *testing.T) {
	c := Config{Base: time.Second, Max: 5 * time.Second}
	for i := 0; i < 200; i++ {
		if d := c.Delay(50); d >= c.Max {
			t.Fatalf("delay %v exceeded max %v", d, c.Max)
		}
	}
}

func TestDelayHandlesHugeAttemptCounts(t *testing.T) {
	// A job that has failed thousands of times must not overflow or panic.
	c := Config{Base: time.Minute, Max: time.Hour}
	for _, n := range []int{31, 64, 1000, 1 << 20} {
		if d := c.Delay(n); d < 0 || d >= c.Max {
			t.Fatalf("attempts=%d: delay %v out of range", n, d)
		}
	}
}

func TestDelayActuallyVaries(t *testing.T) {
	// Sanity check that jitter is present: 50 draws should not all be equal.
	c := Config{Base: time.Second, Max: time.Minute}
	first := c.Delay(3)
	for i := 0; i < 50; i++ {
		if c.Delay(3) != first {
			return
		}
	}
	t.Fatal("50 draws were identical; jitter is not being applied")
}
