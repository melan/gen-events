package output

import (
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/melan/gen-events/events_generator"
)

type EventsPublisher interface {
	Init()
	Publish(events []events_generator.Event)
	Cleanup()
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
