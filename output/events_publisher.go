package output

import (
	"sync"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/melan/gen-events/events_generator"
)

type EventsPublisher interface {
	Init() error
	Publish(events []events_generator.Event)
	Cleanup(g *sync.WaitGroup)
}

type PublisherFactory func(org *events_generator.Org) EventsPublisher

func CreateKinesisPublisherFactory(client *kinesis.Kinesis, tags map[string]*string) PublisherFactory {
	return func(org *events_generator.Org) EventsPublisher {
		return NewKinesisEventsPublisher(client, org.StreamName(), org.NumberOfStreamShards(), tags)
	}
}

func CreateFilePublisherFactory(outputDir string) PublisherFactory {
	return func(org *events_generator.Org) EventsPublisher {
		return NewFileEventsPublisher(outputDir, org)
	}
}
