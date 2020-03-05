package tracker

type TrackerInterface interface {
	UseMemoryTracker() bool
	UpdatePosition(processorName string, key string, value int)
	GetPosition(processorName string, key string) int
}
