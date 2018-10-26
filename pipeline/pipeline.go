package pipeline

import (
	"time"

	"github.com/melan/gen-events/events_generator"
	"github.com/melan/gen-events/misc"
	"github.com/melan/gen-events/output"
	"github.com/prometheus/client_golang/prometheus"
)

type Pipeline struct {
	publisher output.EventsPublisher
	org       *events_generator.Org
	interval  time.Duration
}

func NewPipeline(publisher output.EventsPublisher, org *events_generator.Org, interval time.Duration) *Pipeline {
	return &Pipeline{
		publisher: publisher,
		org:       org,
		interval:  interval,
	}
}

func (p *Pipeline) Pump() {
	labelsNames := []string{"orgSize", "caseId", "orgId"}
	labels := prometheus.Labels{}
	labels["orgSize"] = string(p.org.OrgSize)
	labels["caseId"] = string(p.org.CaseId)
	labels["orgId"] = p.org.OrgId

	cyclesCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "cycles_count",
			Help:      "Count how many cycles of events generation was performed",
		},
		labelsNames).With(labels)

	eventsCountGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "events_per_cycle",
			Help:      "How many events were generated per cycle",
		},
		labelsNames).With(labels)

	prometheus.MustRegister(cyclesCounter, eventsCountGauge)

	for {
		go func() {
			events := p.org.GenerateEvents()
			p.publisher.Publish(events)
			eventsCountGauge.Set(float64(len(events)))
			cyclesCounter.Add(1)
		}()
		time.Sleep(p.interval)
	}

}
