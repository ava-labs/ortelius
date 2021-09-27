// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import "time"

type RetrySleeper struct {
	counter          uint64
	retries          uint64
	sleepTime        time.Duration
	sleepDuration    time.Duration
	sleepDurationMax time.Duration
}

func NewRetrySleeper(retries uint64, sleepDuration time.Duration, sleepDurationMax time.Duration) *RetrySleeper {
	return &RetrySleeper{
		retries:          retries,
		sleepTime:        0,
		sleepDuration:    sleepDuration,
		sleepDurationMax: sleepDurationMax,
	}
}

func (r *RetrySleeper) Inc() {
	r.counter++
	if r.counter > r.retries {
		if r.sleepTime < r.sleepDuration {
			r.sleepTime += r.sleepDuration
		}
		time.Sleep(r.sleepTime)
	}
}
