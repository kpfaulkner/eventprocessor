package tracker

import (
	"sync"
)

// NullTracker... for when we dont actually want to track.
// Saves putting a bunch of logic elsewhere... yes, slightly less efficient
// but can deal with it.
type NullTracker struct {
}

var nullTracker *NullTracker
var nullOnce sync.Once

func NewNullTracker(path string, syncIntervalInMS int ) *NullTracker {
	nullOnce.Do(func() {
		nullTracker = &NullTracker{}
	})
  return nullTracker
}

func (t *NullTracker) Enabled() bool {
  return true
}

func (t *NullTracker) UseMemoryTracker() bool {
	return false
}

func (t *NullTracker) UpdatePosition(processName string, key string, value int) error {
	return nil
}

func (t *NullTracker) GetPosition(processorName string, key string) int{
  return -1 // valid, right?
}


