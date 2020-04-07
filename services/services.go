package services

import (
	"github.com/ava-labs/gecko/ids"
)

type Ingestable interface {
	ID() ids.ID
	ChainID() ids.ID
	Body() []byte
}

// Service takes in Ingestables and adds them to the services backend
type Service interface {
	Add(Ingestable) error
}

// FanOutService takes in items and sends them to multiple backend Services
type FanOutService []Service

// Add takes in an Ingestable and sends it to all underlying backends
func (fos FanOutService) Add(i Ingestable) (err error) {
	for _, service := range fos {
		if err = service.Add(i); err != nil {
			return err
		}
	}
	return nil
}
