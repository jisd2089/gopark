package streaming

/**
    Author: luzequan
    Created: 2018-07-27 18:26:08
*/
import (
	"gopkg.in/oleiade/lane.v1"
	"time"
	"fmt"
	"github.com/cznic/mathutil"
)

const (
	STOP  = iota
	EVENT
)

type item struct {
	key   int
	value time.Time
}

type Scheduler struct {
	ssc        *StreamingContext
	graph      *DStreamGraph
	jobManager *JobManager
	timer      *RecurringTimer
	queue      *lane.Deque
}

func (s *Scheduler) init(ssc *StreamingContext) {
	s.ssc = ssc
	s.graph = ssc.graph
	s.jobManager = &JobManager{}
	s.timer = &RecurringTimer{period: ssc.batchDuration, callback: s.generateEvent}
	s.queue = lane.NewDeque()
}

func newScheduler(ssc *StreamingContext) *Scheduler {
	scheduler := &Scheduler{}
	scheduler.init(ssc)
	return scheduler
}

func (s *Scheduler) start(t time.Time) {
	s.graph.start(t)
	s.timer.start(t.UnixNano())
}

func (s *Scheduler) stop() {
	s.timer.stop()
	s.graph.Stop()
	s.queue.Append(&item{STOP, nil})
}

func (s *Scheduler) generateEvent(t time.Time) {
	s.queue.Append(&item{EVENT, t})
}

func (s *Scheduler) runOnce() bool {
	first := s.queue.First()
	if first == nil {
		return false
	}

	var key int
	var time time.Time
	switch first.(type) {
	case *item:
		key = first.(*item).key
		time = first.(*item).value
	default:
		return false
	}

	if key == STOP {
		return true
	}

	s.generateRDDs(time)
	return false
}

func (s *Scheduler) generateRDDs(t time.Time) {
	for _, job := range s.graph.GenerateRDDs(t) {
		fmt.Println("start to run job %s", job.time)
		s.jobManager.runJob(job)
	}
	s.graph.ForgetOldRDDs(t)
	s.ssc.doCheckpoint(t)
}

type RecurringTimer struct {
	period   time.Duration
	callback func(t time.Time)
	stopped  bool
	nextTime int64
}

func (r *RecurringTimer) start(start int64) {
	r.nextTime = ((start / int64(r.period)) + 1) * int64(r.period)
	r.stopped = false
	go r.run()
}

func (r *RecurringTimer) stop() {
	r.stopped = true
}

func (r *RecurringTimer) run() {
	for {
		if r.stopped {
			break
		}

		now := time.Now()
		if now.UnixNano() >= r.nextTime {
			fmt.Println("start call %s with %d (delayed %f)", r.callback,
				r.nextTime, now.UnixNano()-int64(r.nextTime))

			r.callback()
			r.nextTime += int64(r.period)
		} else {
			min := mathutil.Min(int(r.nextTime-now.UnixNano()), 1)
			max := mathutil.Max(min, 0.01)
			time.Sleep(time.Duration(max))
		}
	}
}
