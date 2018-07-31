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

type Scheduler struct {
	ssc        *StreamingContext
	graph      *DStreamGraph
	jobManager *JobManager
	timer      *RecurringTimer
	queue      lane.Deque
}

func (s *Scheduler) start(t time.Time) {
	s.graph.start(t)
	s.timer.start(t.UnixNano())
}

type RecurringTimer struct {
	period   time.Duration
	callback func()
	stopped  bool
	nextTime int64
}

func (r *RecurringTimer) start(start int64) {
	r.nextTime = ((start / int64(r.period)) + 1) * int64(r.period)
	r.stopped = false
	go r.run()
}

func (r *RecurringTimer) stop(start time.Time) {
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
				r.nextTime, now.UnixNano() - int64(r.nextTime))

			r.callback()
			r.nextTime += int64(r.period)
		} else {
			min := mathutil.Min(int(r.nextTime - now.UnixNano()), 1)
			max := mathutil.Max(min, 0.01)
			time.Sleep(time.Duration(max))
		}
	}
}