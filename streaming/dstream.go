package streaming

/**
    Author: luzequan
    Created: 2018-07-27 18:27:48
*/
import (
	"github.com/mijia/gopark"
	"time"
	"sync"
	"path"
	"os"
	"strings"
)

type MapperFunc func(interface{}) interface{}
type PartitionMapperFunc func(Yielder) Yielder
type FlatMapperFunc func(interface{}) []interface{}
type ReducerFunc func(interface{}, interface{}) interface{}
type FilterFunc func(interface{}) bool
type LoopFunc func(interface{})

type DStream interface {
	Union(other DStream) DStream
	Map(fn MapperFunc) DStream
	FlatMap(fn FlatMapperFunc) DStream
	Filter(fn FilterFunc) DStream
	Glom() DStream
	MapPartitions(fn PartitionMapperFunc, preserve bool) DStream
	Reduce(fn ReducerFunc) DStream
	Count() DStream
	Foreach(fn LoopFunc) DStream
	Transform(fn MapperFunc) DStream
	Show() DStream
	Window(duration time.Duration, slideDuration time.Duration) DStream
	Slice(beginTime time.Time, endTime time.Time) []gopark.RDD
	Tumble(batch time.Duration) DStream
	ReduceByWindow() DStream
	CountByWindow(windowDuration time.Duration, slideDuration time.Duration) DStream
	DefaultPartitioner()
	GroupByKey()
	ReduceByKey(fn ReducerFunc) DStream
	CombineByKey() DStream
	CountByKey()
	GroupByKeyAndWindow() DStream
	ReduceByKeyAndWindow() DStream
	CountByKeyAndWindow() DStream
	UpdateStateByKey() DStream
	MapValues(fn MapperFunc) DStream
	FlatMapValues(fn MapperFunc) DStream
	Cogroup() DStream
	Join() DStream

	setContext(context *StreamingContext)
	zeroTime() *time.Time
	register()
	parentRememberDuration() time.Duration
	setGraph(g *DStreamGraph)
	remember(duration time.Duration)
	isTimeValid(t time.Time) bool
	compute(time time.Time) gopark.RDD
	generateJob(time time.Time) *Job
	forgetOldRDDs(time time.Time)
	updateCheckpointData(time time.Time)
	restoreCheckpointData()
}

type baseDStream struct {
	ssc                *StreamingContext
	prototype          DStream
	slideDuration      time.Duration
	dependencies       []DStream
	generatedRDDs      map[int64]gopark.RDD
	rememberDuration   time.Duration
	mustCheckpoint     bool
	lastCheckpointTime *time.Time
	checkpointData     map[string]gopark.RDD
	graph              *DStreamGraph
}

func (s *baseDStream) init(ssc *StreamingContext, prototype DStream) {
	s.ssc = ssc
	s.prototype = prototype
	s.dependencies = make([]DStream, 0)
	s.generatedRDDs = make(map[int64]gopark.RDD)
	s.mustCheckpoint = false
	//s.lastCheckpointTime = 0
	s.checkpointData = make(map[string]gopark.RDD)
}

func (s *baseDStream) getOrCompute(time time.Time) gopark.RDD {

	if rdds, ok := s.generatedRDDs[time.UnixNano()]; ok {
		return rdds
	}
	if s.isTimeValid(time) {
		rdd := s.compute(time)
		s.generatedRDDs[time.UnixNano()] = rdd
		if s.ssc.checkpointDuration > 0 && time > (s.lastCheckpointTime+Time(s.ssc.checkpointDuration)) {
			s.lastCheckpointTime = time

			if rdd != nil {
				//	TODO
			}
			return rdd
		}
	}
	return nil
}

func (s *baseDStream) generateJob(time time.Time) *Job {
	rdd := s.getOrCompute(time)
	if rdd != nil {
		//job := &Job{}
		//s.ssc.sc
	}
	return nil
}

func (s *baseDStream) forgetOldRDDs(time time.Time) {
}

func (s *baseDStream) updateCheckpointData(time time.Time) {
}

func (s *baseDStream) restoreCheckpointData() {
}

func (s *baseDStream) setContext(context *StreamingContext) {
	s.ssc = context
	for _, dep := range s.dependencies {
		dep.setContext(context)
	}
}

func (s *baseDStream) zeroTime() *time.Time {
	return s.graph.zeroTime
}

func (s *baseDStream) register() {

}

func (s *baseDStream) parentRememberDuration() time.Duration {
	return s.rememberDuration
}

func (s *baseDStream) setGraph(g *DStreamGraph) {
	s.graph = g
	for _, dep := range s.dependencies {
		dep.setGraph(g)
	}
}

func (s *baseDStream) remember(duration time.Duration) {
	if duration > 0 && (s.rememberDuration == 0 || duration > s.rememberDuration) {
		s.rememberDuration = duration
	}
	for _, dep := range s.dependencies {
		dep.remember(s.parentRememberDuration())
	}
}

func (s *baseDStream) isTimeValid(t time.Time) bool {
	return false
}

func (s *baseDStream) compute(time time.Time) gopark.RDD {
	return nil
}

func (s *baseDStream) Union(other DStream) DStream {
	return nil
}

func (s *baseDStream) Map(fn MapperFunc) DStream {
	return nil
}

func (s *baseDStream) FlatMap(fn FlatMapperFunc) DStream {
	return nil
}

func (s *baseDStream) Filter(fn FilterFunc) DStream {
	return nil
}

func (s *baseDStream) Glom() DStream {
	return nil
}

func (s *baseDStream) MapPartitions(fn PartitionMapperFunc, preserve bool) DStream {
	return nil
}

func (s *baseDStream) Reduce(fn ReducerFunc) DStream {
	return nil
}

func (s *baseDStream) Count() DStream {
	return nil
}

func (s *baseDStream) Foreach(fn LoopFunc) DStream {
	return nil
}

func (s *baseDStream) Transform(fn MapperFunc) DStream {
	return nil
}

func (s *baseDStream) Show() DStream {
	return nil
}

func (s *baseDStream) Window(duration time.Duration, slideDuration time.Duration) DStream {
	return nil
}

func (s *baseDStream) Slice(beginTime time.Time, endTime time.Time) []gopark.RDD {
	return nil
}

func (s *baseDStream) Tumble(batch time.Duration) DStream {
	return nil
}

func (s *baseDStream) ReduceByWindow() DStream {
	return nil
}

func (s *baseDStream) CountByWindow(windowDuration time.Duration, slideDuration time.Duration) DStream {
	return nil
}

func (s *baseDStream) DefaultPartitioner() {

}

func (s *baseDStream) GroupByKey() {

}

func (s *baseDStream) ReduceByKey(fn ReducerFunc) DStream {
	return nil
}

func (s *baseDStream) CombineByKey() DStream {
	return nil
}

func (s *baseDStream) CountByKey() {

}

func (s *baseDStream) GroupByKeyAndWindow() DStream {
	return nil
}

func (s *baseDStream) ReduceByKeyAndWindow() DStream {
	return nil
}

func (s *baseDStream) CountByKeyAndWindow() DStream {
	return nil
}

func (s *baseDStream) UpdateStateByKey() DStream {
	return nil
}

func (s *baseDStream) MapValues(fn MapperFunc) DStream {
	return nil
}

func (s *baseDStream) FlatMapValues(fn MapperFunc) DStream {
	return nil
}

func (s *baseDStream) Cogroup() DStream {
	return nil
}

func (s *baseDStream) Join() DStream {
	return nil
}

type DerivedDStream struct {
}

type MappedDStream struct {
	*DerivedDStream
}

type FlatMappedDStream struct {
	*DerivedDStream
}

type FilteredDStream struct {
	*DerivedDStream
}

type MapValuedDStream struct {
	*DerivedDStream
}

type FlatMapValuedDStream struct {
	*DerivedDStream
}

type GlommedDStream struct {
	*DerivedDStream
}

type MapPartitionedDStream struct {
	*DerivedDStream
}

type TransformedDStream struct {
	*DerivedDStream
}

type ForEachDStream struct {
	*DerivedDStream
}

type StateDStream struct {
	*DerivedDStream
}

type UnionDStream struct {
	*baseDStream
}

func (u *UnionDStream) init(parents []DStream) {
}

type WindowedDStream struct {
	windowDuration time.Duration
	slideDuration  time.Duration
	dependencies   []DStream
}

func (w *WindowedDStream) parentRememberDuration() time.Duration {
	return 0
}

type CoGroupedDStream struct {
	*DerivedDStream
}

type ShuffledDStream struct {
	*DerivedDStream
}

type ReducedWindowedDStream struct {
	*DerivedDStream
}

type InputDStream struct {
	*baseDStream
	dependencies  []DStream
	slideDuration time.Duration
}

func (i *InputDStream) init(ssc *StreamingContext) {
	i.baseDStream.init(ssc, i)
	i.dependencies = make([]DStream, 0)
	i.slideDuration = ssc.batchDuration
}

func (i *InputDStream) start() {

}

func (i *InputDStream) stop() {

}

type ConstantInputDStream struct {
	*InputDStream
}

type ModTimeAndRangeFilter struct {
	lastModTime   time.Time
	latestModTime time.Time
	oldThreshold  int
	accessedFiles map[string]int
	oldFiles      []string
}

func (m *ModTimeAndRangeFilter) init(lastModTime time.Time, oldThreshold int) {
	m.lastModTime = lastModTime
	m.latestModTime = time.Now()
	m.oldThreshold = oldThreshold
	m.accessedFiles = make(map[string]int, 0)
	m.oldFiles = make([]string, 0)
}

func (m *ModTimeAndRangeFilter) call(path string) {
	if _, err := os.Stat(path); err != nil{
		if os.IsNotExist(err){
			return
		}
	}

	for _, val := range m.oldFiles {
		if strings.EqualFold(val, path) {
			return
		}
	}


}

func (m *ModTimeAndRangeFilter) rotate() {
	m.lastModTime = m.latestModTime
}

type FileInputDStream struct {
	*InputDStream
	directory string
}

func (n *FileInputDStream) init(ssc *StreamingContext, directory string, newFilesOnly bool, oldThreshold int) {
	n.InputDStream.init(ssc)
	n.directory = directory

}

func newFileInputDStream() DStream {
	return
}

type RotatingFilesInputDStream struct {
	*InputDStream
}

type QueueInputDStream struct {
	*InputDStream
}

type NetworkInputDStream struct {
	*InputDStream
	fn       func()
	messages []byte
	lock     sync.RWMutex
}

func (n *NetworkInputDStream) init(ssc *StreamingContext, fn func()) {
	n.InputDStream.init(ssc)
	n.fn = fn
	n.messages = make([]byte, 0)
}

func newNetworkInputDStream(ssc *StreamingContext, fn func()) DStream {
	networkInputDStream := &NetworkInputDStream{}
	networkInputDStream.init(ssc, fn)
	return networkInputDStream
}

type SocketInputDStream struct {
	*InputDStream
	hostname string
	port     int
}

type ScribeInputDStream struct {
	*NetworkInputDStream
	zkAddress string
	zkPath    string
	receive   func()
	category  string
}

func (s *ScribeInputDStream) init(ssc *StreamingContext, zkAddress string, zkPath string, category string) {
	s.NetworkInputDStream.init(ssc, s.receive)
	s.zkAddress = zkAddress
	s.zkPath = zkPath
	s.category = category
}

func newScribeInputDStream(ssc *StreamingContext, zkAddress string, zkPath string, category string) DStream {
	scribeInputDStream := &ScribeInputDStream{}
	scribeInputDStream.init(ssc, zkAddress, zkPath, category)
	return scribeInputDStream
}
