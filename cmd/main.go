package main

import (
	"fmt"
	"github.com/kpfaulkner/eventprocessor/cmd/ep"
	"github.com/kpfaulkner/eventprocessor/pkg/eventprocessors"
	"github.com/kpfaulkner/eventprocessor/pkg/streamprocessor"
)

func main() {

	// where the tracking DB will be stored.
	trackerPath := "c:/temp/mytracker"
	processor := ep.NewSalesEventProcessor()
	l := []eventprocessors.EventProcessor { &processor}
	err := streamprocessor.StartProcessingStream("Default", trackerPath, l, 500)
  if err != nil {
  	fmt.Printf("Couldn't start StartProcessingStream %s\n", err.Error())
  	return
  }
}
