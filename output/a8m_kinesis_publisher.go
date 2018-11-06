package output

import (
	"context"
	"sync"
	"time"

	"github.com/a8m/kinesis-producer"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/melan/gen-events/events_generator"
	log "github.com/sirupsen/logrus"
)

type a8mEventsPublisher struct {
	publisher        *producer.Producer
	kinesisPublisher EventsPublisher
	ctx              context.Context
	cancel           context.CancelFunc
}

func (p *a8mEventsPublisher) Init() error {
	if err := p.kinesisPublisher.Init(); err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			case r := <-p.publisher.NotifyFailures():
				p.publisher.Put(r.Data, r.PartitionKey)
			}
		}
	}()

	p.publisher.Start()

	return nil
}

func (p *a8mEventsPublisher) Publish(events []events_generator.Event) {
	for _, e := range events {
		jsEvent, err := e.ToJson()
		if err != nil {
			continue
		}
		p.publisher.Put(jsEvent, e.PartitionKey())
	}
}

func (p *a8mEventsPublisher) Cleanup(g *sync.WaitGroup) {
	p.cancel()
	p.publisher.Stop()
	p.kinesisPublisher.Cleanup(g)
}

func NewA8mKinesisPublisher(client *kinesis.Kinesis, kinesisStream string, shards int64, tags map[string]*string) EventsPublisher {
	ctx, cancel := context.WithCancel(context.Background())

	var putter producer.Putter = client

	return &a8mEventsPublisher{
		publisher: producer.New(&producer.Config{
			StreamName:    kinesisStream,
			BacklogCount:  2000,
			Client:        putter,
			FlushInterval: guessIntervalSec(shards),
			Logger:        log.WithField("stream", kinesisStream),
		}),
		kinesisPublisher: NewKinesisEventsPublisher(client, kinesisStream, shards, tags),
		ctx:              ctx,
		cancel:           cancel,
	}

}

func guessIntervalSec(shards int64) time.Duration {
	return time.Duration(5.0 / float32(shards) * float32(time.Millisecond))
}
