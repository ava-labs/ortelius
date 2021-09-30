// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

type Running interface {
	Close()
	IsStopped() bool
}

type running struct {
	quitCh chan struct{}
}

func NewRunning() Running {
	return &running{quitCh: make(chan struct{})}
}

func (r *running) Close() {
	close(r.quitCh)
}

func (r *running) IsStopped() bool {
	select {
	case <-r.quitCh:
		return true
	default:
		return false
	}
}
