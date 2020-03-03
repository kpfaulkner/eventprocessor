package main

import (
	"fmt"
	"github.com/kpfaulkner/eventprocessor/cmd/ep"
	"github.com/kpfaulkner/eventprocessor/pkg/eventprocessors"
	"github.com/kpfaulkner/eventprocessor/pkg/streamprocessor"
	"log"
	"net/http"
	_ "net/http/pprof"

)

func main() {

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// where the tracking DB will be stored.
	trackerPath := "c:/temp/mytracker"
	processor := ep.NewSalesEventProcessor()
	l := []eventprocessors.EventProcessor { &processor}
	err := streamprocessor.StartProcessingStream(true, "admin", "changeit", "localhost", "1113", "Default", trackerPath, l, 500)
  if err != nil {
  	fmt.Printf("Couldn't start StartProcessingStream %s\n", err.Error())
  	return
  }
}
