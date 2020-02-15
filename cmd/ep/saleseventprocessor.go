package ep

import (
	"encoding/json"
	"fmt"
	"github.com/jdextraze/go-gesclient/client"
	"github.com/kpfaulkner/eventprocessor/pkg/eventprocessors"
)

const (
	SalesEventId = "SalesEventId"
	SalesEventId2 = "SalesEventId2"
)

type IncomingAmount struct {
	Amount int `json:"amount"`
}

type SalesEventProcessor struct {
	eventprocessors.BaseEventProcessor
	totalSales int
	count int
}

func NewSalesEventProcessor() SalesEventProcessor{
	s := SalesEventProcessor{}
	s.EventTypes = []string{ SalesEventId}
  s.ProcessorName = "SalesEventProcessor"
	s.CanSkipEvent = true
	s.totalSales = 0
	s.count = 0
	return s
}

func (s *SalesEventProcessor) ProcessEvent(e *client.ResolvedEvent) error {
	switch e.Event().EventType() {
	case SalesEventId:
		return s.ProcessEventSalesEventId(e)
	case SalesEventId2:
		return s.ProcessEventSalesEventId2(e)
	}
	return nil
}

func (s *SalesEventProcessor) ProcessEventSalesEventId(e *client.ResolvedEvent) error {
	amount := IncomingAmount{}
	json.Unmarshal(e.Event().Data(), &amount)
	s.totalSales += amount.Amount
	//fmt.Printf("Incoming sales amount %d, Total sales amount %d\n", amount.Amount, s.totalSales)
	s.count++

	if s.count % 1000 == 0 {
		fmt.Printf("count %d\n", s.count)
	}
	return nil
}

func (s *SalesEventProcessor) ProcessEventSalesEventId2(e *client.ResolvedEvent) error {
	amount := IncomingAmount{}
	json.Unmarshal(e.Event().Data(), &amount)
	s.totalSales += amount.Amount
	fmt.Printf("Incoming sales amount %d, Total sales amount %d\n", amount.Amount, s.totalSales)
	return nil
}
