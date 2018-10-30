package pipeline

import (
	"time"

	"github.com/melan/gen-events/events_generator"
	"github.com/melan/gen-events/misc"
	"github.com/melan/gen-events/output"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	cyclesCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "cycles_count",
			Help:      "Count how many cycles of events generation was performed",
		},
		[]string{"orgSize", "caseId", "orgId"})
	eventsCountGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "events_per_cycle",
			Help:      "How many events were generated per cycle",
		},
		[]string{"orgSize", "caseId", "orgId"})
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
	labels := prometheus.Labels{}
	labels["orgSize"] = string(p.org.OrgSize)
	labels["caseId"] = string(p.org.CaseId)
	labels["orgId"] = p.org.OrgId

	for {
		go func() {
			events := p.org.GenerateEvents()
			p.publisher.Publish(events)
			eventsCountGauge.With(labels).Set(float64(len(events)))
			cyclesCounter.With(labels).Add(1)
		}()
		time.Sleep(p.interval)
	}

}
