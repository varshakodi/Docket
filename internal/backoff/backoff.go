// Package backoff decides how long to wait before retrying a failed job.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Config tunes the retry schedule.
//
// The wait grows exponentially with each failed attempt (Base, 2*Base,
// 4*Base, ...) and is capped at Max, so a job that keeps failing backs off
// quickly at first, then settles at a steady retry rate.
type Config struct {
	Base time.Duration // wait after the first failure; default 1s
	Max  time.Duration // never wait longer than this; default 5m
}

// Delay returns how long to wait before the next try, given how many
// attempts have already been made.
//
// The result is randomised: a uniform pick from [0, cap) rather than cap
// itself. This is "full jitter", and it matters more than it looks. When an
// upstream service goes down, every in-flight job fails at the same moment.
// Without jitter they would all retry at the same moment too, hit the
// recovering service in one synchronised wave, and fail together again --
// a retry storm that can keep a service down after it has recovered.
// Spreading the retries randomly across the window breaks that lockstep.
func (c Config) Delay(attempts int) time.Duration {
	base, max := c.Base, c.Max
	if base <= 0 {
		base = time.Second
	}
	if max <= 0 {
		max = 5 * time.Minute
	}

	// Shifting left by n multiplies by 2^n. Cap the shift so we cannot
	// overflow int64 -- 2^30 seconds is 34 years, far beyond any sane Max.
	shift := attempts
	if shift > 30 {
		shift = 30
	}
	if shift < 0 {
		shift = 0
	}
	ceiling := base << uint(shift)
	if ceiling <= 0 || ceiling > max { // <= 0 catches overflow to negative
		ceiling = max
	}

	return time.Duration(rand.Int64N(int64(ceiling)))
}
