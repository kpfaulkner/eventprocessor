package tracker

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
)

type TrackerDetails struct {
	Key string  `json:"Key"`
	Value int   `json:"Value"`
}


// JsonTracker just reads/writes the data directly to a local file.
// Seeing if this is any quicker than bboltdb
type JsonTracker struct {
	jsonPath string
	lock sync.RWMutex  // have multiple goroutines writing at once.... let's be

	// stores the actual info.
	positionMap map[string]TrackerDetails
}

var jsonTracker *JsonTracker
var jsonOnce sync.Once

func NewJsonTracker(path string) *JsonTracker {
	jsonOnce.Do(func() {
		jsonTracker = &JsonTracker{}
		jsonTracker.jsonPath = path
		var err error
		jsonTracker.positionMap,err = deserialiseFromDisk(path)
		if err != nil {
			// panic....  only safe thing to do.
			panic("Unable to load tracker data")
		}
	})
  return jsonTracker
}


func (t *JsonTracker) serialiseToDisk() error {
	b, err := json.Marshal(t.positionMap)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(t.jsonPath, b, 0777)
	// handle this error
	if err != nil {
		return err
	}
	return nil
}

func deserialiseFromDisk(jsonPath string) (map[string]TrackerDetails, error) {
	f, err := os.Open(jsonPath)
	if err != nil {
		return nil,err
	}
	defer f.Close()

	var m map[string]TrackerDetails
	jsonParser := json.NewDecoder(f)
	jsonParser.Decode(&m)

	if m == nil {
		// no valid contents of file... just make an empty map
		m = make(map[string]TrackerDetails)
	}

	return m,nil
}


func (t *JsonTracker) UpdatePosition(processName string, key string, value int) error {
	var err error

	// mutex to handle all the goroutines writing.
	t.lock.Lock()
	var cache TrackerDetails
	var ok bool

	if cache, ok = t.positionMap[processName]; !ok {
		cache = TrackerDetails{ key, value}
	} else {
		cache.Value = value
	}
	t.positionMap[processName] = cache

	// now write to disk.
	t.serialiseToDisk()
	t.lock.Unlock()
	return err
}

// GetPostition get data from json file
func (t *JsonTracker) GetPosition(processorName string, key string) int{
  return t.positionMap[processorName].Value
}

// probably need to modify interface to get rid of this.
func (t *JsonTracker) UseMemoryTracker() bool {
	return false
}
