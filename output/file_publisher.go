package output

import (
	"bytes"
	"log"
	"os"
	"path/filepath"

	"github.com/melan/gen-events/events_generator"
)

var newLineBytes = []byte("\n")

type FileEventsPublisher struct {
	fileName string
}

func (p *FileEventsPublisher) Init() {
}

func (p *FileEventsPublisher) Publish(events []events_generator.Event) {
	f, err := os.OpenFile(p.fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("can't open file %s. Error: %s", p.fileName, err)
	}

	jsons := make([][]byte, 0, len(events))
	for _, event := range events {
		js, err := event.ToJson()
		if err != nil {
			log.Printf("can't serialize event %#v. skipping", event)
			continue
		}

		jsons = append(jsons, js)
	}

	var batch []byte
	batch = bytes.Join(jsons, []byte("\n"))
	if len(batch) > 0 {
		batch = append(batch, "\n"...)
	}
	_, err = f.Write(batch)

	if err != nil {
		log.Fatalf("can't write to file %s because of an error: %s", p.fileName, err)
	}

	defer f.Close()
}

func (p *FileEventsPublisher) Cleanup() {
}

func NewFileEventsPublisher(outputPath string, org *events_generator.Org) EventsPublisher {
	filePath := filepath.Join(outputPath, org.StreamName())
	return &FileEventsPublisher{
		fileName: filePath,
	}
}
