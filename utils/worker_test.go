// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"sync"
	"testing"
	"time"
)

type Job struct {
	val int
}

func NewJob(val int) *Job {
	return &Job{val: val}
}

func TestWorker(t *testing.T) {
	for itest := 0; itest < 5; itest++ {
		lock := sync.Mutex{}
		valmap := make(map[int]int)
		f := func(wn int, job interface{}) {
			lock.Lock()
			defer lock.Unlock()
			if j, ok := job.(*Job); ok {
				valmap[j.val] = j.val
			}
		}

		var icnt int

		w := NewWorker(10, 2, f)
		for icnt = 0; icnt < 100; icnt++ {
			w.Enque(NewJob(icnt))
		}
		w.Finish(100 * time.Millisecond)

		lock.Lock()
		defer lock.Unlock()

		if len(valmap) != 100 {
			t.Errorf("value map invalid length %d", len(valmap))
		}

		for icnt = 0; icnt < 100; icnt++ {
			if ok := valmap[icnt]; ok != icnt {
				t.Errorf("valmap[%d] not found", icnt)
			}
		}
	}
}
