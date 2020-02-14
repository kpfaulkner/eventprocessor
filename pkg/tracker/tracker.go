package tracker

import (
	"encoding/binary"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"sync"

)

type TrackerKeyValue struct {
	Key []byte
	Value []byte
}

// Tracker tracks/stores where each processor is up to (event wise).
// Using bboltDB for this...
// Have a bbolt bucket per processor.
type Tracker struct {
	db *bolt.DB
	dbPath string
}

var tracker Tracker
var once sync.Once

func NewTracker(path string ) Tracker {
	once.Do(func() {
		tracker = Tracker{}
		tracker.dbPath = path
		db, err := bolt.Open(path, 0666, nil)
		if err != nil {
			log.Fatalf("unable to open bbolt db %s\n", err.Error())
		}
		tracker.db = db
	})

  return tracker
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
func (t *Tracker) UpdatePosition(bucketName string, key string, value int) error {
	eventNo := make([]byte, 4)
	eventNoInt := uint32(value)
	binary.LittleEndian.PutUint32(eventNo, eventNoInt)
	err := t.Update(bucketName, []byte(key), eventNo)
	return err
}

// Update updates a key in a given bucket. key and value are byte arrays.
// This is due to bbolt will require this in the first place, and it means
// that I dont have to create individual functions per value type
// It is expected that the bucket is actually the stream name (may change).
func (t *Tracker) Update(bucketName string, key []byte, value []byte) error{
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

	// -1 means no data recorded for it.
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


