package streamprocessor

import (
	"fmt"
	"github.com/avast/retry-go"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/kpfaulkner/eventprocessor/pkg/eventprocessors"
	"github.com/kpfaulkner/eventprocessor/pkg/tracker"
	"sync"
	"time"
)

type EventProcessorChannelPair struct {
  processor eventprocessors.EventProcessor
  channel chan client.ResolvedEvent
}

// StreamProcessor processes a single stream.
type StreamProcessor struct {
	connection client.Connection
	subscription client.CatchUpSubscription
	streamName string

	// array of processors for launching/shutting down
	processorPairs []EventProcessorChannelPair

	// map of processors... to make mapping to eventtypes easier.
  processorMap map[string][]EventProcessorChannelPair

	// tracker used to keep track of where we are up to (eventids) of for each processor.
	tracker tracker.TrackerInterface
}

// NewStreamProcessor create new StreamProcessor for reading a given stream and processing IT ALL!!
// syncIntervalInMS can have a big perf impact (potentially).
// value of 0 means it writes to the tracker realtime... as events are processed
// If > 0 then we write to the tracker persistent storage every <whatever> milliseconds.
// This means that far fewer writes to storage (speed!!!) but it also means that if an event is processed
// and then the application crashes (so tracker isn't persisted to disk) there is a risk of events being
// reprocessed the next time its written. Sooo, this means you need to be aware if the event processors being executed can
// handle multiple runs for the same event.
func NewStreamProcessor(streamName string, processors []eventprocessors.EventProcessor, trackerPath string, syncIntervalInMS int) StreamProcessor {
  c := StreamProcessor{}
  c.streamName = streamName

  // used to track where processes are up to?
  c.tracker = tracker.NewBoltTracker(trackerPath, syncIntervalInMS)
	//c.tracker = tracker.NewJsonTracker(trackerPath)
  //c.tracker.CreateBucket(streamName)

  // map between event and a processor channel pair.
  // also list of processors.
  c.processorMap, c.processorPairs = registerAllProcessors(processors)
  return c
}

// LaunchAllProjections is fire off goroutine for each projection.
func (c *StreamProcessor)  LaunchAllProcessors(wg *sync.WaitGroup) error {
	for _,pp := range c.processorPairs {
		wg.Add(1)
		go c.launchProcessor(pp, wg)
	}
	return nil
}

// ShutdownAllProjections closes the channels and will shutdown all goroutines.
func (c *StreamProcessor)  ShutdownAllProjections() error {
	for _,pp := range c.processorPairs {
		close(pp.channel)
	}
	return nil
}

// loadLastProcessedEventNumberPerProcessor loads (from bboltdb) where each
// processor was up to.
func (c *StreamProcessor) loadLastProcessedEventNumberPerProcessor() error {
	for _,p := range c.processorPairs {
		minVal := c.tracker.GetPosition(p.processor.GetProcessorName(), "position")
		// also into processor itself. Possibly not needed.
		err := p.processor.SetLastEventNumber(minVal)
		if err != nil {
			return err
		}
	}
	return nil
}

// getMinimumEventNumberForStreamProcessors looks at all the processors used for this EventStoreConsumer
// (which all use the same stream) and determines the minimum event no used for the processors.
// This means that processing will start from this minimum number and any processor that has already
// processed that event no will simply skip it.
func (c *StreamProcessor) getMinimumEventNumberForStreamProcessors() int {
	minimumEventNo := -1
	for _,pp := range c.processorPairs {
		minVal := pp.processor.GetLastEventNumber()
		if minimumEventNo == -1 {
			minimumEventNo = minVal
			continue
		}

		if minVal < minimumEventNo {
			minimumEventNo = minVal
		}
	}
	return minimumEventNo
}

func processEventWithRetry(processor eventprocessors.EventProcessor, e *client.ResolvedEvent) error {
	err := retry.Do(
		func() error {
			return processor.ProcessEvent(e)
		}, retry.Delay( 100 * time.Millisecond),
	)
	return err
}

// launchProjection ... simply a helper function to launch a goroutine, read the channel and send the event
// data to the projection.
func (c *StreamProcessor) launchProcessor(pp EventProcessorChannelPair, wg *sync.WaitGroup) error {

	for true {
		req, ok := <- pp.channel
		if !ok {
			// closed...  so break out.
			break
		}

		// only process if the event number is greater than what the processor has already done.
		// should probably be storing this in p.processor memory as opposed to hitting storage
		// But in reality it appears as if the writing is the bottleneck and not this
		// reading. Will only go back and re-evaluate this IF it really turns out to
		// have some significant impact.
		currentCount := c.tracker.GetPosition(pp.processor.GetProcessorName(), "position")
		if currentCount < req.OriginalEventNumber() {
			// wrap processing in a retry loop.
			err := processEventWithRetry(pp.processor, &req)
			if err == nil || pp.processor.CanSkipBadEvent() {
				if err != nil && pp.processor.CanSkipBadEvent() {
					fmt.Printf("Cannot process event %d with processor %s, skipping it\n", req.OriginalEventNumber(), pp.processor.GetProcessorName())
				}
				c.tracker.UpdatePosition(pp.processor.GetProcessorName(), "position", req.OriginalEventNumber())
			} else {
				if !pp.processor.CanSkipBadEvent() {
					// cannot skip... what do we do? kill the processor?
					fmt.Printf("Cannot process event %d, have error %s killing processor %s!\n", req.OriginalEventNumber(), err.Error(), pp.processor.GetProcessorName())
					break
				}
			}
		} else {
			fmt.Printf("Processor %s has already processed event %d\n", pp.processor.GetProcessorName(), req.OriginalEventNumber())
		}
	}

	wg.Done()
	return nil
}

// register all processors. Add them to a event type based map.
// returns a map with key of eventtype (just string) to a slice of EventProcessorChannelPair. Each EventProcessorChannelPair is a
// channel (to send the data) and the event processor used to process it.
func registerAllProcessors( eventProcessors []eventprocessors.EventProcessor) ( map[string][]EventProcessorChannelPair, []EventProcessorChannelPair) {
	ppMap := make(map[string][]EventProcessorChannelPair)
  ppArray := []EventProcessorChannelPair{}

	for _,p := range eventProcessors {
		pp := EventProcessorChannelPair{processor: p }
		ch := make(chan client.ResolvedEvent , 10000)
		pp.channel = ch
		ppArray = append(ppArray, pp)

		// put
		for _,et := range p.GetRegisteredEventTypes() {
			ppArray, ok := ppMap[et]
			if !ok {
				ppArray = []EventProcessorChannelPair{}
			}
			ppMap[et] = append(ppArray, pp)
		}
	}
	return ppMap, ppArray
}

// StartEventStoreConsumerForStream starts a number of EventProcessors against a particular stream
func StartProcessingStream(usessl bool, username string, password string, server string, port string, streamName string, trackerPath string, processors []eventprocessors.EventProcessor, trackerSyncIntervalInMS int ) error {

	sp := NewStreamProcessor(streamName, processors, trackerPath, trackerSyncIntervalInMS)

	// make sure each processor knows where it was up to.
	// we might be able to get rid of this since the tracker should really take care of this.
	err := sp.loadLastProcessedEventNumberPerProcessor()
	if err != nil {
		return err
	}

	// determine the absolute minimum event we should start processing from.
	lastCheckpoint := sp.getMinimumEventNumberForStreamProcessors()
	var fromEventNumber *int

	// -1 as lastcheckpoint means that its never been used (uninitialised).
	if lastCheckpoint > -1 {
		fromEventNumber = &lastCheckpoint
	}

	// CatchupSubscriberManager has the raw connection to EventStore. It will read the event and
	// pub it onto the appropriate channel.
	csc := NewCatchupSubscriberManager(sp.processorMap, usessl, username, password, server, port)
	err = csc.ConnectCatchupSubscriberConnection(streamName, fromEventNumber )
	if err != nil {
		fmt.Printf("KABOOM %s\n", err.Error())
		return err
	}

	// start processing all the channels.
	wg := sync.WaitGroup{}
	sp.LaunchAllProcessors(&wg)
	wg.Wait()

	return nil
}


