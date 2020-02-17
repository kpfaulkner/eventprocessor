package tracker

import (
	"encoding/binary"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"sync"
	"time"
)

type TrackerKeyValue struct {
	Key []byte
	Value []byte
}

type MemoryTrackerKeyValue struct {
	Key string
	Value int
	Stored bool
}

// Tracker tracks/stores where each processor is up to (event wise).
// Using bboltDB for this...
// Have a bbolt bucket per processor.
type Tracker struct {
	db *bolt.DB
	dbPath string
	memoryTracker map[string]MemoryTrackerKeyValue
	useMemoryTracker bool
	syncIntervalInMS int
}

var tracker Tracker
var once sync.Once

// if syncIntervalInMS == 0 it means write realtime and use boltDB as normal.
// if > 0 then write to memory then sync every syncinterval.
func NewTracker(path string, syncIntervalInMS int ) Tracker {
	once.Do(func() {
		tracker = Tracker{}
		tracker.dbPath = path
		db, err := bolt.Open(path, 0666, nil)
		if err != nil {
			log.Fatalf("unable to open bbolt db %s\n", err.Error())
		}
		tracker.db = db
		tracker.syncIntervalInMS = syncIntervalInMS
		if syncIntervalInMS > 0 {
			// going to write to internal tracker (map) then sync every now and then :)
			tracker.memoryTracker = make(map[string]MemoryTrackerKeyValue)
			tracker.useMemoryTracker = true
      go tracker.sync()
		} else {
			tracker.useMemoryTracker = false
		}

	})

  return tracker
}

// Sync gets called every t.syncIntervalInMS and writes out map to boltdb
func (t *Tracker) sync() {
	for {
		// write out each modified (stored == false) entry to persistent storage.
		for k, v := range t.memoryTracker {
			if !v.Stored {
				eventNo := make([]byte, 4)
				eventNoInt := uint32(v.Value)
				binary.LittleEndian.PutUint32(eventNo, eventNoInt)
				err := t.updatePersistedStorage(k, []byte("position"), eventNo)
				if err != nil {
					log.Fatalf("Unable to persist to storage... stopping %s\n", err.Error())
				}
				v.Stored = true
				t.memoryTracker[k] = v
			}
		}
		time.Sleep( time.Duration(t.syncIntervalInMS) * time.Millisecond)
	}
}

func (t *Tracker) Connect() error{

	db, err := bolt.Open(t.dbPath, 0666, nil)
	if err != nil {
		return err
	}

	t.db = db
	return nil
}

func (t *Tracker) CreateBucket(bucketName string) error{
	t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		var err error
		if b == nil {
			b, err = tx.CreateBucket([]byte(bucketName))
			if err != nil {
				fmt.Printf("Cannot create bucket %s : %s\n", bucketName, err.Error())
				return err
			}
		}
		return err
	})

	return nil
}

func (t *Tracker) Close() error{
	err := t.db.Close()
	return err
}


// UpdatePosition updates a key in a given bucket..
// assumption key is string and value is int. Does the byte array conversion dance.
// If t.useMemoryTracker is true, then write to the Tracker Map..  this will get synced later.
// if t.useMemoryTracker is false, just write to boltdb directly.
func (t *Tracker) UpdatePosition(bucketName string, key string, value int) error {
	var err error
	if t.useMemoryTracker {

		var cache MemoryTrackerKeyValue
		var ok bool
		// TODO(kpfaulkner) confirm thread safe. I believe maps are thread safe (for same key)... is so, this
		// should be good. If not...  lock or channels.
		// Dont bother referencing "key", since that's always position.
		if cache, ok = t.memoryTracker[bucketName]; !ok {
			cache = MemoryTrackerKeyValue{ key, value, false}
		} else {
			cache.Value = value
			cache.Stored = false // been updated... so not written to disk.
		}
		t.memoryTracker[bucketName] = cache
	} else {
		eventNo := make([]byte, 4)
		eventNoInt := uint32(value)
		binary.LittleEndian.PutUint32(eventNo, eventNoInt)
		err = t.updatePersistedStorage(bucketName, []byte(key), eventNo)
	}
	return err
}

// updatePersistedStorage updates a key in a given bucket. key and value are byte arrays.
// This is due to bbolt will require this in the first place, and it means
// that I dont have to create individual functions per value type
// It is expected that the bucket is actually the stream name (may change).
func (t *Tracker) updatePersistedStorage(bucketName string, key []byte, value []byte) error{
	t.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		var err error
		if b == nil {
			b, err = tx.CreateBucket([]byte(bucketName))
			if err != nil {
				fmt.Printf("Cannot create bucket %s : %s\n", bucketName, err.Error())
				return err
			}
		}

		err = b.Put(key, value)
		return err
	})

	return nil
}

func (t *Tracker) GetInt(bucketName string, key string) int{

	keybytes := []byte(key)
	var val []byte
	t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b != nil {
			val = b.Get(keybytes)
		}
		return nil
	})

	eventNo := -1
	if len(val) != 0 {
		eventNo = int(binary.LittleEndian.Uint32(val))
	}
	return eventNo
}

func (t *Tracker) Get(bucketName string, key []byte) []byte{

	var val []byte
	t.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b != nil {
			val = b.Get(key)
		}

		return nil
	})
	return val
}


func (t *Tracker) GetKeysForBucket(bucketName string) [][]byte{

	keyList := make([][]byte,0,5)
	t.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(bucketName))

		c := b.Cursor()
		for k, _:= c.First(); k != nil; k, _ = c.Next() {
			keyList = append(keyList, k)
		}
		return nil
	})

	return keyList
}

func (t *Tracker) GetKeyValueListForBucket(bucketNames []string) []TrackerKeyValue{

	l := make([]TrackerKeyValue,0,5)
	for _,bucket := range bucketNames {
		t.db.View(func(tx *bolt.Tx) error {
			// Assume bucket exists and has keys
			b := tx.Bucket([]byte(bucket))

			c := b.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				kv := TrackerKeyValue{k, v}
				l = append(l, kv)
			}
			return nil
		})
	}
	return l
}


