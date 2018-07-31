package streaming

/**
    Author: luzequan
    Created: 2018-07-27 18:18:52
*/
import (
	"time"
	"fmt"
)

type JobManager struct {
}

func (m *JobManager) runJob(job *Job) {
	fmt.Println("start to run job %s", job)
	used := job.run()
	delayed := time.Now().Sub(job.time)
	fmt.Println("job used %s, delayed %s", used, delayed)
}

type Job struct {
	time time.Time
	fn   func()
}

func (j *Job) run() time.Duration {
	start := time.Now()
	j.fn()
	end := time.Now()
	return end.Sub(start)
}
