package tracker

type TrackerInterface interface {
	UseMemoryTracker() bool
	UpdatePosition(processorName string, key string, value int) error
	GetPosition(processorName string, key string) int
}
