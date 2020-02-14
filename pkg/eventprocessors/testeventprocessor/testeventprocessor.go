package testeventprocessor

import (
	"fmt"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/kpfaulkner/eventprocessor/pkg/eventprocessors"
)

const (
	TestEvent1Id = "TestEvent1Id"
	TestEvent2Id = "TestEvent2Id"
)

type TestProcessor struct {
	eventprocessors.BaseEventProcessor
}

func NewTestProcessor() TestProcessor{
	p := TestProcessor{}
	p.EventTypes = []string{ TestEvent1Id, TestEvent2Id}
	//p.StreamName = "Default"
	p.ProcessorName = "TestProcessor"
	p.CanSkipEvent = true
	return p
}

func (p *TestProcessor) ProcessEvent(e *client.ResolvedEvent) error {

	et :=  e.Event().EventType()
	// figure out event type, and send it onwards :)
	switch et {
	case TestEvent1Id:
		fmt.Printf("TestEvent1Id received")
	case TestEvent2Id:
		fmt.Printf("TestEvent2Id received")

	default:
		fmt.Printf("known type %s\n",e.Event().EventType() )
	}

	return nil
}
