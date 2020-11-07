package streamprocessor

import (
	"flag"
	"fmt"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/jdextraze/go-gesclient/flags"
	"log"
)

type CatchupSubscriberManager struct {
	server string
	username string
	password string
	port string
	usessl bool
	connection client.Connection
	subscription client.CatchUpSubscription
	streamName string
	processorMap map[string][]EventProcessorChannelPair
	eventTypeChannelMap map[string][]chan client.ResolvedEvent
}

func NewCatchupSubscriberManager(processorMap map[string][]EventProcessorChannelPair,etChannelMap map[string][]chan client.ResolvedEvent, usessl bool,  username string, password string, server string, port string) CatchupSubscriberManager {
	csm := CatchupSubscriberManager{}
	csm.processorMap = processorMap
	csm.eventTypeChannelMap = etChannelMap
	csm.username = username
	csm.password = password
	csm.port = port
	csm.server = server
	csm.usessl = usessl
	return csm
}

func (c *CatchupSubscriberManager) ConnectCatchupSubscriberConnection(streamName string, fromEventNumber *int ) error {

	fs := flag.NewFlagSet("esflags", flag.ExitOnError)
	flags.Init(fs)

	var schema string
	if c.usessl  {
		schema = "ssl"
	} else {
		schema = "tcp"
	}

	connectionString := fmt.Sprintf("%s://%s:%s@%s:%s",schema, c.username, c.password, c.server, c.port)
	fs.Set("endpoint", connectionString)

	if c.usessl {
		fs.Set("ssl-host", connectionString)
	}
	flag.Parse()

	conn, err := flags.CreateConnection("AllCatchupSubscriber")
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}

	if err := conn.ConnectAsync().Wait(); err != nil {
		log.Fatalf("Error connecting: %v", err)
	}

	c.connection = conn
	c.streamName = streamName
	settings := client.NewCatchUpSubscriptionSettings(client.CatchUpDefaultMaxPushQueueSize,
																										client.CatchUpDefaultReadBatchSize, flags.Verbose(), true)

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
func (c *CatchupSubscriberManager) processEvent(_ client.CatchUpSubscription, e *client.ResolvedEvent) error {
	//fmt.Printf("event appeared: %+v | %s\n", e, string(e.OriginalEvent().Data()))

	// If a processor is interested in a given EventType, then pass it to its channel.
	// This will mean potentially a little bit of double handling, one check here to figure out if it
	// should go into the channel, and then the processor itself will need to determine (switch/ifs)
	// how to react to each EventType, but it's probably worth it.

	// get the event type, get the list of process/channel pairs registered for that event type
	// populate channels.
	et := e.Event().EventType()
	//processors,ok := c.processorMap[et]
	channelsToSend, ok := c.eventTypeChannelMap[et]
	if ok {
		for _,ch := range channelsToSend {
			ch <- *e
		}
	}

	return nil
}

func (c *CatchupSubscriberManager) liveProcessingStarted(_ client.CatchUpSubscription) error {
	log.Println("Live processing started")
	return nil
}

func (c *CatchupSubscriberManager) subscriptionDropped(_ client.CatchUpSubscription, r client.SubscriptionDropReason, err error) error {
	log.Printf("subscription dropped: %s, %v", r, err)
	return nil
}



