package ep

import (
	"fmt"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/kpfaulkner/eventprocessor/pkg/eventprocessors"
)

const (
	TestEvent1Id = "TestEvent1Id"
	Test2Event2Id = "Test2Event2Id"
)

// This is to just test out having eventprocessors in command project rather than
// framework section.
type Test2Processor struct {
	eventprocessors.BaseEventProcessor

}

func NewTest2Processor() Test2Processor{
	p := Test2Processor{}
	p.EventTypes = []string{ TestEvent1Id, Test2Event2Id}
  //p.StreamName = "Default"
	p.ProcessorName = "Test2Processor"
	p.CanSkipEvent = false
	return p
}

func (p *Test2Processor) ProcessEvent(e *client.ResolvedEvent) error {

	et :=  e.Event().EventType()

	// figure out event type, and send it onwards :)
	switch et {
	case TestEvent1Id:
		fmt.Printf("YYYYYYYYY TestEvent1Id received")
	case Test2Event2Id:
		fmt.Printf("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX        Test2Event2Id received")

	default:
		fmt.Printf("known type %s\n",e.Event().EventType() )
	}

	return nil
}
