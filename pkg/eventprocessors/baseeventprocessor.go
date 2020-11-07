package eventprocessors

import (
	"github.com/avast/retry-go"
	"github.com/jdextraze/go-gesclient/client"
)

func Contains( l []string, e string) bool {
	for _, a := range l {
		if a == e {
			return true
		}
	}
	return false
}

type EventProcessor interface {
	ProcessEvent(event *client.ResolvedEvent) error

	// eventtypes will just be strings.
	// initially used string alias as EventType but meant
	// lots of conversion. Can't see it worth it atm.
	GetRegisteredEventTypes() []string

	GetProcessorName() string

	// if this processor cannot process an event, can we skip it?
	CanSkipBadEvent() bool

	GetLastEventNumber() int

	SetLastEventNumber(pos int) error

	// GetNumberOfInstances returns number of instances of this EventProcessor can be created
	// All these instances will feed off the same channel of events, so the load is spread
	// over these instances and NOT duplicated.
	GetNumberOfInstances() int
}

// BaseEventProcessor, base struct for all event processors.
// Currently has assumption that an EventProcessor will only be used for a single stream.
// Unsure how valid this currently is, but working assumption for now.
type BaseEventProcessor struct {
	Name string   // EventProcessor name
	EventTypes []string
	RetryConfig retry.Config // retry config, used by ProcessEventWithRetry
	ProcessorName string
	CanSkipEvent bool
	LastEventProcessed int
	NumberOfInstances int
}

func (p *BaseEventProcessor) ProcessEvent(event *client.ResolvedEvent) error {
	panic("Base class, method not implemented")
}

func (p *BaseEventProcessor) GetRegisteredEventTypes() []string {
	return p.EventTypes
}

func (p *BaseEventProcessor) GetProcessorName() string {
	return p.ProcessorName
}

func (p *BaseEventProcessor) CanSkipBadEvent() bool {
	return p.CanSkipEvent
}

func (p *BaseEventProcessor) GetLastEventNumber() int {
	return p.LastEventProcessed
}

func (p *BaseEventProcessor) SetLastEventNumber(pos int) error {
	p.LastEventProcessed = pos
	return nil
}

func (p *BaseEventProcessor) GetNumberOfInstances() int {
	return p.NumberOfInstances
}
