// (c) 2021, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"sync"
	"time"
)

type Worker interface {
	Enque(job interface{})
	TryEnque(job interface{})
	Finish(sleepTime time.Duration)
	JobCnt() int64
	IsFinished() bool
}

type worker struct {
	jobCh     chan interface{}
	doneCh    chan bool
	processor func(int, interface{})
	wgWorker  sync.WaitGroup
	queueCnt  int
}

func NewWorker(queueSize int, queueCnt int, processor func(int, interface{})) Worker {
	w := worker{
		queueCnt:  queueCnt,
		jobCh:     make(chan interface{}, queueSize),
		doneCh:    make(chan bool),
		processor: processor,
		wgWorker:  sync.WaitGroup{},
	}

	var iproc int
	for iproc = 0; iproc < queueCnt; iproc++ {
		w.wgWorker.Add(1)
		go w.worker(iproc)
	}

	return &w
}

func (w *worker) worker(wn int) {
	defer w.wgWorker.Done()
	for {
		select {
		case update := <-w.jobCh:
			w.processor(wn, update)
		case <-w.doneCh:
			return
		}
	}
}

func (w *worker) Enque(job interface{}) {
	w.jobCh <- job
}

func (w *worker) TryEnque(job interface{}) {
	select {
	case w.jobCh <- job:
	default:
	}
}

func (w *worker) Finish(sleepTime time.Duration) {
	for !w.IsFinished() {
		time.Sleep(sleepTime)
	}
	w.Close()
}

func (w *worker) Close() {
	close(w.doneCh)
	w.wgWorker.Wait()
}

func (w *worker) IsFinished() bool {
	return w.JobCnt() == 0
}

func (w *worker) JobCnt() int64 {
	return int64(len(w.jobCh))
}
