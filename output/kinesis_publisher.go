package output

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/melan/gen-events/events_generator"
	"github.com/melan/gen-events/misc"
	"github.com/prometheus/client_golang/prometheus"
)

type KinesisEventsPublisher struct {
	client                  *kinesis.Kinesis
	kinesisStream           string
	generatedEventsCounter  prometheus.Counter
	serializedEventsCounter prometheus.Counter
	serializedEventsSize    prometheus.Gauge
	shards                  int64
	tags                    map[string]*string
}

func NewKinesisEventsPublisher(client *kinesis.Kinesis, kinesisStream string, shards int64, tags map[string]*string) EventsPublisher {
	publisher := &KinesisEventsPublisher{
		client:        client,
		kinesisStream: kinesisStream,
		shards:        shards,
		tags:          tags,
		generatedEventsCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: misc.MetricsPrefix,
				Name:      "generated_events_count",
				Help:      "Number of events received for posting",
			},
			[]string{"stream"}).WithLabelValues(kinesisStream),

		serializedEventsCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: misc.MetricsPrefix,
				Name:      "serialized_events_count",
				Help:      "Number of events successfully serialized",
			},
			[]string{"stream"}).WithLabelValues(kinesisStream),

		serializedEventsSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: misc.MetricsPrefix,
				Name:      "serialized_events_size",
				Help:      "Data volume of serialized events",
			},
			[]string{"stream"}).WithLabelValues(kinesisStream),
	}

	prometheus.MustRegister(publisher.serializedEventsSize, publisher.generatedEventsCounter,
		publisher.serializedEventsCounter)

	return publisher
}

func (p *KinesisEventsPublisher) Init() error {
	describeStreamInput := &kinesis.DescribeStreamInput{
		StreamName: &p.kinesisStream,
	}

	for {
		_, err := p.client.DescribeStream(describeStreamInput)
		if err != nil {
			awsErr, ok := err.(awserr.Error)
			if !ok {
				return fmt.Errorf("can't describe stream %s. Unexpected error: %s", p.kinesisStream, err.Error())
			}

			switch awsErr.Code() {
			case "ResourceNotFoundException":
				// create stream
				p.createStream(p.shards)
				for {
					if err := p.client.WaitUntilStreamExists(describeStreamInput); err != nil {
						log.Printf("something is wrong while waiting for stream %s/%d to be created, will check later. Error: %s",
							p.kinesisStream, p.shards, err.Error())
						time.Sleep(1 * time.Second)
					}
					p.setTags(p.tags)
					break
				}
				continue
			case "LimitExceededException":
				time.Sleep(1 * time.Second)
				continue
			default:
				// unknown error. panic
				return fmt.Errorf("can't describe stream %s. Unexpected AWS error: %s", p.kinesisStream, err.Error())
			}
		}

		// TODO: Add some kind of resize if more/less shards than needed
		return nil
	}
}

func (p *KinesisEventsPublisher) Publish(events []events_generator.Event) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, len(events))
	var totalSize int64

	p.generatedEventsCounter.Add(float64(len(events)))

	for _, event := range events {
		jsEvent, err := event.ToJson()
		if err != nil {
			continue
		}
		partitionKey := event.PartitionKey()
		totalSize += int64(len(jsEvent) + len([]byte(partitionKey)))

		records = append(records, &kinesis.PutRecordsRequestEntry{
			Data:         jsEvent,
			PartitionKey: &partitionKey,
		})
	}

	p.serializedEventsCounter.Add(float64(len(records)))
	p.serializedEventsSize.Set(float64(totalSize))

	batches := make([][]*kinesis.PutRecordsRequestEntry, 0, len(records)/500)
	batchSizeLimit := int(4.5 * math.Pow(2, 20)) // 4.5 MB

	if len(records) < 500 && totalSize < int64(batchSizeLimit) {
		batches = append(batches, records)
	} else {
		batch := make([]*kinesis.PutRecordsRequestEntry, 0, 500)
		var batchSize int

		for i := 0; i < len(records); i++ {
			record := records[i]
			recordSize := len(record.Data) + len([]byte(*record.PartitionKey))

			if len(batch) == 500 || batchSize+recordSize >= batchSizeLimit { // reset batch
				batches = append(batches, batch)
				batch = make([]*kinesis.PutRecordsRequestEntry, 0, 500)
				batchSize = 0
				i-- // reprocess the record
			} else {
				batch = append(batch, record)
				batchSize += recordSize
			}
		}
	}

	for _, batch := range batches {
		go func(batch []*kinesis.PutRecordsRequestEntry) {
			retryBatch := make([]*kinesis.PutRecordsRequestEntry, len(batch))
			copy(retryBatch, batch)

			for len(retryBatch) > 0 { // keep trying while there is anything to post

				input := &kinesis.PutRecordsInput{
					Records:    retryBatch,
					StreamName: &p.kinesisStream,
				}

				res, err := p.client.PutRecords(input)
				if err != nil {
					log.Printf("Can't send batch to %s - Skipping. %s", p.kinesisStream, err.Error())
					return
				}

				if res.FailedRecordCount == nil || *res.FailedRecordCount == 0 { // everything is fine
					return
				}

				newRetryBatch := make([]*kinesis.PutRecordsRequestEntry, 0, *res.FailedRecordCount)
				wasThrottled := false
				for i, record := range res.Records {
					if record.ShardId != nil { // this record was posted already. Skipping
						continue
					}

					if record.ErrorCode != nil && *record.ErrorCode == "ProvisionedThroughputExceededException" {
						wasThrottled = true
					}

					newRetryBatch = append(newRetryBatch, retryBatch[i])
				}

				retryBatch = newRetryBatch
				if wasThrottled { // wait 1/4 of second to avoid throttling
					time.Sleep(250 * time.Millisecond)
				}
			}
		}(batch)
	}
}

func (p *KinesisEventsPublisher) createStream(shardsCount int64) {
	createRequest := &kinesis.CreateStreamInput{
		StreamName: &p.kinesisStream,
		ShardCount: &shardsCount,
	}
	for {
		_, err := p.client.CreateStream(createRequest)
		if err != nil {
			awsErr, ok := err.(awserr.Error)
			if !ok {
				log.Panicf("can't create a new stream %s/%d because of unexpected error: %s",
					p.kinesisStream, shardsCount, err.Error())
			}

			switch awsErr.Code() {
			case "ResourceInUseException":
				log.Printf("new stream %s/%d was created but it is in unknown state",
					p.kinesisStream, shardsCount)
				return
			case "LimitExceededException":
				log.Printf("can't create a new stream %s/%d because of throttling on aws side",
					p.kinesisStream, shardsCount)
				time.Sleep(100 * time.Millisecond)
				continue
			case "InvalidArgumentException":
				// something is wrong. panic
				log.Panicf("can't create a new stream %s/%d because one of arguments is invalid. Error: %s",
					p.kinesisStream, shardsCount, err.Error())
			default:
				// something is wrong. panic
				log.Panicf("can't create a new stream %s/%d because of unexpected AWS Error: %s",
					p.kinesisStream, shardsCount, err.Error())
			}
		} else {
			return
		}
	}
}

func (p *KinesisEventsPublisher) setTags(tags map[string]*string) {
	tagsRequest := &kinesis.AddTagsToStreamInput{
		StreamName: &p.kinesisStream,
		Tags:       tags,
	}
	for {
		_, err := p.client.AddTagsToStream(tagsRequest)
		if err != nil {
			awsErr, ok := err.(awserr.Error)
			if !ok {
				log.Panicf("can't add tags to stream %s, tags: %#v. because of unexpected error: %s",
					p.kinesisStream, tags, err.Error())
			}

			switch awsErr.Code() {
			case "ResourceNotFoundException":
				log.Panicf("can't find stream %s to add tags. Error: %s",
					p.kinesisStream, err.Error())

			case "ResourceInUseException":
				log.Printf("stream %s isn't ready yet to add tags, will try later. Error: %s",
					p.kinesisStream, err.Error())
				time.Sleep(1 * time.Second)

			case "InvalidArgumentException":
				log.Panicf("wrong parameter while adding tags to stream %s. tags: %v, Error: %s",
					p.kinesisStream, tags, err.Error())

			case "LimitExceededException":
				log.Printf("request to add tags to stream %s was throttled. Will try later. Error: %s",
					p.kinesisStream, err.Error())
				time.Sleep(1 * time.Second)
				continue

			default:
				log.Panicf("can't add tags to stream %s. tags: %#v. because of unexpected AWS Error: %s",
					p.kinesisStream, tags, err.Error())
			}
		} else {
			return
		}
	}

}

func (p *KinesisEventsPublisher) Cleanup(g *sync.WaitGroup) {
	defer g.Done()

	trueTrue := true
	deleteRequest := &kinesis.DeleteStreamInput{
		EnforceConsumerDeletion: &trueTrue,
		StreamName:              &p.kinesisStream,
	}

	for {
		_, err := p.client.DeleteStream(deleteRequest)
		if err != nil {
			awsErr, ok := err.(awserr.Error)
			if !ok {
				log.Printf("can't remove stream %s because of unexpected error: %s", p.kinesisStream, err.Error())
				return
			}

			switch awsErr.Code() {
			case "ResourceNotFoundException":
				// stream doesn't exists
				return
			case "LimitExceededException":
				log.Printf("request to delete stream %s was throttled, will try later. Error: %s",
					p.kinesisStream, err.Error())
				time.Sleep(100 * time.Millisecond)
				continue
			case "ResourceInUseException":
				log.Printf("stream %s is busy, will try later. Error: %s",
					p.kinesisStream, err.Error())
				time.Sleep(100 * time.Millisecond)
				continue
			default:
				log.Printf("can't remove stream %s because of unexpected AWS error: %s", p.kinesisStream, err.Error())
			}
		} else {
			p.client.WaitUntilStreamNotExists(&kinesis.DescribeStreamInput{
				StreamName: &p.kinesisStream,
			})
			return
		}
	}
}
