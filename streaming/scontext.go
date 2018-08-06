package streaming

import (
	"fmt"
	"path"
	"github.com/mijia/gopark"
	"os"
	"encoding/gob"
	"time"
	"sync"
)

/**
    Author: luzequan
    Created: 2018-07-27 18:17:54
*/

var WITH_SCRIBE = false

type Yielder chan interface{}
type ReducerFn func(yield Yielder, partition int) interface{}

type StreamingContext struct {
	sc                 *gopark.Context
	batchDuration      time.Duration
	graph              *DStreamGraph
	checkpointDir      string
	checkpointDuration time.Duration
	scheduler          *Scheduler
	lastCheckpointTime time.Time
	batchCallback      func()
}

func (c *StreamingContext) init(sc *gopark.Context, batchDuration time.Duration, graph *DStreamGraph, batchCallback func()) {
	c.sc = sc
	c.batchDuration = batchDuration
	c.graph = graph
	//c.lastCheckpointTime = 0
	c.batchCallback = batchCallback
}

func (c *StreamingContext) load(path string) (*StreamingContext, *time.Time) {
	cp := &Checkpoint{}
	if err := cp.read(path); err != nil {
		fmt.Println(err)
	}

	graph := cp.graph
	ssc := &StreamingContext{
		batchDuration: cp.batchDuration,
		//sc.master
		graph:              graph,
		checkpointDir:      path,
		checkpointDuration: cp.checkpointDuration,
	}
	graph.SetContext(ssc)
	graph.RestoreCheckpointData()

	return ssc, cp.time
}

func (c *StreamingContext) remember(duration time.Duration) {
	c.graph.Remember(duration)
}

func (c *StreamingContext) checkpoint(directory string, interval time.Duration) {
	c.checkpointDir = directory
	c.checkpointDuration = interval
}

func (c *StreamingContext) registerInputStream(ds DStream) {
	c.graph.AddInputStream(ds)
}

func (c *StreamingContext) registerOutputStream(ds DStream) {
	c.graph.AddOutputStream(ds)
}

func (c *StreamingContext) networkTextStream(hostname string, port int) DStream {
	ds := &SocketInputDStream{
		hostname: hostname,
		port:     port,
	}
	c.registerInputStream(ds)
	return ds
}

func (c *StreamingContext) scribeTextStream(zk_address string, zk_path string) DStream {
	if WITH_SCRIBE {
		ds := newScribeInputDStream(c, zk_address, zk_path, "default")
		c.registerInputStream(ds)
		return ds
	}
	return nil
}

func (c *StreamingContext) customStream(fn func()) DStream {
	ds := newNetworkInputDStream(c, fn)
	c.registerInputStream(ds)
	return ds
}

func (c *StreamingContext) fileStream(directory string, newFilesOnly bool, oldThreshold int64) DStream {
	ds := newFileInputDStream(c, directory, newFilesOnly, oldThreshold)
	c.registerInputStream(ds)
	return ds
}

func (c *StreamingContext) rotatingFiles(files []string) DStream {
	ds := newRotatingFilesInputDStream(c, files)
	c.registerInputStream(ds)
	return ds
}

func (c *StreamingContext) textFileStream(directory string, newFilesOnly bool, oldThreshold int64) DStream {
	return c.fileStream(directory, newFilesOnly, oldThreshold).Map(func(x interface{}) []interface{} {
		keyValue := x.(*gopark.KeyValue)
		values := keyValue.Value.([]interface{})
		results := make([]interface{}, len(values))
		for i := range values {
			results[i] = &gopark.KeyValue{
				Key:   keyValue.Key,
				Value: values[i],
			}
		}
		return results
	})
}

func (c *StreamingContext) makeStream(rdd gopark.RDD) DStream {
	return newConstantInputDStream(c, rdd)
}

func (c *StreamingContext) queueStream(queue []gopark.RDD, oneAtAtime bool, defaultRDD gopark.RDD) DStream {
	ds := newQueueInputDStream(c, queue, oneAtAtime, defaultRDD)
	c.registerInputStream(ds)
	return ds
}

func (c *StreamingContext) union(streams []DStream) DStream {
	return newUnionDStream(streams)
}

func (c *StreamingContext) start(t time.Time) {
	c.sc.Start()
	for _, ds := range c.graph.inputStreams {
		switch ds.(type) {
		case *NetworkInputDStream:
			ds.(*NetworkInputDStream).startReceiver()
		}
	}

	c.scheduler = newScheduler(c)
	c.scheduler.start(t)
}

func (c *StreamingContext) runOnce() bool {
	return c.scheduler.runOnce()
}

func (c *StreamingContext) awaitTermination(timeout time.Duration) {

}

func (c *StreamingContext) stop() {
	if c.scheduler != nil {
		c.scheduler.stop()
	}
}

func (c *StreamingContext) doCheckpoint(time time.Time) {
	if c.checkpointDuration > 0 && time.After(c.lastCheckpointTime.Add(c.checkpointDuration)) {
		c.lastCheckpointTime = time
		c.graph.UpdateCheckpointData(time)
		newCheckpoint(c, time).write(c.checkpointDir)
	}
}

type Interval struct {
	begin time.Time
	end   time.Time
}

func (i *Interval) duration() time.Duration {
	return i.end.Sub(i.begin)
}

func (i *Interval) add(d time.Duration) *Interval {
	i.begin = i.begin.Add(d)
	i.end = i.end.Add(d)
	return i
}

func (i *Interval) sub(d time.Duration) *Interval {
	i.begin = i.begin.Add(-d)
	i.end = i.end.Add(-d)
	return i
}

func (i *Interval) le(that *Interval) bool {
	//TODO assert
	if i.duration() == that.duration() {

	}
	return i.begin.Before(that.begin)
}

func (i *Interval) ge(that *Interval) bool {
	return false
}

func (i *Interval) str() string {
	return fmt.Sprintf("[%s, %s]", i.begin, i.end)
}

func (i *Interval) current(d time.Duration) *Interval {
	now := time.Now()
	end := now.Add(d)
	return &Interval{
		begin: now,
		end:   end,
	}
}

type DStreamGraph struct {
	inputStreams     []DStream
	outputStreams    []DStream
	zeroTime         *time.Time
	startTime        *time.Time
	batchDuration    time.Duration
	rememberDuration time.Duration
	mu               sync.RWMutex
}

func (g *DStreamGraph) start(time time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.zeroTime = &time
	for _, out := range g.outputStreams {
		out.remember(g.rememberDuration)
	}
	for _, ins := range g.inputStreams {
		ins.start()
	}
}

func (g *DStreamGraph) restart(time time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.startTime = &time
}

func (g *DStreamGraph) Stop() {
	for _, ins := range g.inputStreams {
		ins.stop()
	}
}

func (g *DStreamGraph) SetContext(ssc *StreamingContext) {
	for _, ins := range g.inputStreams {
		ins.baseDStream.setContext(ssc)
	}

	for _, out := range g.outputStreams {
		out.setContext(ssc)
	}
}

func (g *DStreamGraph) Remember(duration time.Duration) {
	g.rememberDuration = duration
}

func (g *DStreamGraph) AddInputStream(input DStream) {
	input.setGraph(g)
	g.inputStreams = append(g.inputStreams, input)
}

func (g *DStreamGraph) AddOutputStream(output DStream) {
	output.setGraph(g)
	g.outputStreams = append(g.outputStreams, output)
}

func (g *DStreamGraph) GenerateRDDs(time time.Time) []*Job {
	var jobList []*Job
	for _, out := range g.outputStreams {
		out.generateJob(time)
		// TODO
	}
	return jobList
}

func (g *DStreamGraph) ForgetOldRDDs(time time.Time) {
	for _, out := range g.outputStreams {
		out.forgetOldRDDs(time)
	}
}

func (g *DStreamGraph) UpdateCheckpointData(time time.Time) {
	for _, out := range g.outputStreams {
		out.updateCheckpointData(time)
	}
}

func (g *DStreamGraph) RestoreCheckpointData() {
	for _, out := range g.outputStreams {
		out.restoreCheckpointData()
	}
}

type Checkpoint struct {
	//master
	time               *time.Time
	graph              *DStreamGraph
	checkpointDuration time.Duration
	batchDuration      time.Duration
}

func (cp *Checkpoint) init(ssc *StreamingContext, time time.Time) {
	//cp.master
	cp.time = &time
	cp.graph = ssc.graph
	cp.checkpointDuration = ssc.checkpointDuration
	cp.batchDuration = ssc.batchDuration
}

func newCheckpoint(ssc *StreamingContext, time time.Time) *Checkpoint {
	checkpoint := &Checkpoint{}
	checkpoint.init(ssc, time)
	return checkpoint
}

func (cp *Checkpoint) write(filePath string) {
	outputFilePath := path.Join(filePath, "metadata")
	outputFile, err := os.Create(outputFilePath)
	defer outputFile.Close()
	if err != nil {
		fmt.Println("open checkpoint file err: ", err)
		return
	}

	enc := gob.NewEncoder(outputFile)
	if err2 := enc.Encode(cp); err2 != nil {
		fmt.Println("gob encode Checkpoint file err: ", err)
	}
}

func (cp *Checkpoint) read(filePath string) error {
	fileName := path.Join(filePath, "metadata")
	readFile, err := os.Open(fileName)
	defer readFile.Close()
	if err != nil {
		fmt.Println("open checkpoint file err: ", err)
		return err
	}

	dec := gob.NewDecoder(readFile)
	if err2 := dec.Decode(cp); err2 != nil {
		fmt.Println("gob encode Checkpoint file err: ", err)
		return err
	}
	return nil
}

// TODO 日志聚合模块
type ScribeHandler struct {
}
