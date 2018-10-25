package kinesis

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/melan/gen-events/events_generator"
)

func PostEvents(client *kinesis.Kinesis, kinesisStream string, events []events_generator.Event) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, len(events))
	var totalSize int64

	// TODO: Add metrics here

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

	batches := make([][]*kinesis.PutRecordsRequestEntry, 0, len(records)/500)
	batchSizeLimit := int(4.5 * (2 ^ 20)) // 4.5 MB

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
					StreamName: &kinesisStream,
				}

				res, err := client.PutRecords(input)
				if err != nil {
					log.Printf("Can't send batch to %s - Skipping. %s", kinesisStream, err.Error())
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
