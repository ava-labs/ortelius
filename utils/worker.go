package utils

import (
	"sync"
	"sync/atomic"
	"time"
)

type Worker interface {
	Enque(job interface{})
	Finish(sleepTime time.Duration)
}

type worker struct {
	jobCh     chan interface{}
	doneCh    chan bool
	quitCh    map[int]chan bool
	processor func(int, interface{})
	wgWorker  sync.WaitGroup
	queueCnt  int
	jobCnt    *int64
}

func NewWorker(queueSize int, queueCnt int, processor func(int, interface{})) Worker {
	w := worker{
		queueCnt:  queueCnt,
		jobCh:     make(chan interface{}, queueSize),
		doneCh:    make(chan bool),
		quitCh:    make(map[int]chan bool),
		processor: processor,
		wgWorker:  sync.WaitGroup{},
		jobCnt:    new(int64),
	}

	var iproc int
	for iproc = 0; iproc < queueCnt; iproc++ {
		w.wgWorker.Add(1)
		w.quitCh[iproc] = make(chan bool)
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
			atomic.AddInt64(w.jobCnt, -1)
		case <-w.doneCh:
			w.quitCh[wn] <- true
			return
		}
	}
}

func (w *worker) Enque(job interface{}) {
	w.jobCh <- job
	atomic.AddInt64(w.jobCnt, 1)
}

func (w *worker) Finish(sleepTime time.Duration) {
	for {
		if atomic.LoadInt64(w.jobCnt) <= 0 {
			break
		}
		time.Sleep(sleepTime)
	}
	close(w.doneCh)

	var iproc int
	for iproc = 0; iproc < w.queueCnt; iproc++ {
		<-w.quitCh[iproc]

		close(w.quitCh[iproc])
	}

	w.wgWorker.Wait()
}
