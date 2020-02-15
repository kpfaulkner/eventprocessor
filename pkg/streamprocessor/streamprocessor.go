package streamprocessor

import (
	"flag"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/jdextraze/go-gesclient/flags"
	"github.com/kpfaulkner/eventprocessor/pkg/eventprocessors"
	"github.com/kpfaulkner/eventprocessor/pkg/tracker"
	"log"

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
	tracker tracker.Tracker
}

// NewStreamProcessor create new StreamProcessor for reading a given stream and processing IT ALL!!
func NewStreamProcessor(streamName string, processors []eventprocessors.EventProcessor, trackerPath string) StreamProcessor {
  c := StreamProcessor{}
  c.streamName = streamName

  // used to track where processes are up to?
  c.tracker = tracker.NewTracker(trackerPath)
  //c.tracker.Connect()
  c.tracker.CreateBucket(streamName)

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
		minVal := c.tracker.GetInt(p.processor.GetProcessorName(), "position")
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

func (c *StreamProcessor) CreateAllCatchupSubscriberConnection(streamName string) error {

	var stream string
	//var lastCheckpoint int
	flags.Init(flag.CommandLine)
  flag.StringVar(&stream, "stream",streamName, "Stream ID")
  //flag.IntVar(&lastCheckpoint, "lastCheckpoint", -1, "Last checkpoint")
  flag.Parse()

	conn, err := flags.CreateConnection("AllCatchupSubscriber")
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}

	if err := conn.ConnectAsync().Wait(); err != nil {
		log.Fatalf("Error connecting: %v", err)
	}

	c.connection = conn

	// figure this out.
	settings := client.NewCatchUpSubscriptionSettings(client.CatchUpDefaultMaxPushQueueSize,
																										client.CatchUpDefaultReadBatchSize, flags.Verbose(), true)

	err = c.loadLastProcessedEventNumberPerProcessor()
	if err != nil {
		return err
	}

	lastCheckpoint := c.getMinimumEventNumberForStreamProcessors()
	var fromEventNumber *int

	// -1 as lastcheckpoint means that its never been used (uninitialised).
	if lastCheckpoint > -1 {
		fromEventNumber = &lastCheckpoint
	}

	c.subscription, err = conn.SubscribeToStreamFrom(c.streamName, fromEventNumber, settings, c.processEvent, c.liveProcessingStarted, c.subscriptionDropped, nil)
	if err != nil {
		log.Fatalf("Unable to subscribe... BOOM %s\n", err.Error())
	}
	return nil
}

// processEvent runs through all projections and run them against the event.
// DOES this have to run in order?
// Or is it that a specific projection just needs to complete before that same projection
// can run for the next event?
func (c *StreamProcessor) processEvent(_ client.CatchUpSubscription, e *client.ResolvedEvent) error {
	//fmt.Printf("event appeared: %+v | %s\n", e, string(e.OriginalEvent().Data()))

	// If a processor is interested in a given EventType, then pass it to its channel.
	// This will mean potentially a little bit of double handling, one check here to figure out if it
	// should go into the channel, and then the processor itself will need to determine (switch/ifs)
	// how to react to each EventType, but it's probably worth it.

	// get the event type, get the list of process/channel pairs registered for that event type
	// populate channels.
	et := e.Event().EventType()
	processors,ok := c.processorMap[et]
	if ok {
		for _,pp := range processors {
			pp.channel <- *e
		}
	}

	return nil
}

func (c *StreamProcessor) liveProcessingStarted(_ client.CatchUpSubscription) error {
	log.Println("Live processing started")
	return nil
}

func (c *StreamProcessor) subscriptionDropped(_ client.CatchUpSubscription, r client.SubscriptionDropReason, err error) error {
	log.Printf("subscription dropped: %s, %v", r, err)
	return nil
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
		if pp.processor.GetLastEventNumber() < req.OriginalEventNumber() {
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
func StartProcessingStream(streamName string, trackerPath string, processors []eventprocessors.EventProcessor ) error {
	consumer := NewStreamProcessor(streamName, processors, trackerPath)

	// Start reading the stream.
	err := consumer.CreateAllCatchupSubscriberConnection(streamName)
	if err != nil {
		fmt.Printf("KABOOM %s\n", err.Error())
		return err
	}

	// start processing the stream.
	wg := sync.WaitGroup{}
	consumer.LaunchAllProcessors(&wg)
	wg.Wait()

	return nil
}


