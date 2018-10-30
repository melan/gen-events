package pipeline

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/melan/gen-events/events_generator"
	"github.com/melan/gen-events/misc"
	"github.com/melan/gen-events/output"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	labelNames    = []string{"orgSize", "caseId", "orgId"}
	cyclesCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "cycles_count",
			Help:      "Count how many cycles of events generation was performed",
		},
		labelNames)
	eventsCountGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "events_per_cycle",
			Help:      "How many events were generated per cycle",
		},
		labelNames)
	generateTimer = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "time_to_generate_events_millisec",
			Help:      "How long did it take to generate events",
		},
		labelNames)
	publishTimer = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "time_to_publish_events_millisec",
			Help:      "How long did it take to publish events",
		},
		labelNames)
)

type CleanupFunc func(g *sync.WaitGroup)

type Pipeline struct {
	OrgId     string
	publisher output.EventsPublisher
	org       *events_generator.Org
	interval  time.Duration
}

func NewPipeline(publisher output.EventsPublisher, org *events_generator.Org, interval time.Duration) *Pipeline {
	return &Pipeline{
		OrgId:     org.OrgId,
		publisher: publisher,
		org:       org,
		interval:  interval,
	}
}

func (p *Pipeline) Pump(ctx context.Context) {
	labels := prometheus.Labels{}
	labels["orgSize"] = string(p.org.OrgSize)
	labels["caseId"] = string(p.org.CaseId)
	labels["orgId"] = p.org.OrgId

	for {
		select {
		case <-ctx.Done():
			log.Printf("Pipeline for org %s of case %s is over. Exiting", p.org.OrgId, string(p.org.CaseId))
			return
		default:
			go func() {
				start := time.Now().UnixNano()
				events := p.org.GenerateEvents()
				end := time.Now().UnixNano()
				generateTimer.With(labels).Observe(float64(end-start) / 1000)

				start = time.Now().UnixNano()
				p.publisher.Publish(events)
				end = time.Now().UnixNano()
				publishTimer.With(labels).Observe(float64(end-start) / 1000)

				eventsCountGauge.With(labels).Set(float64(len(events)))
				cyclesCounter.With(labels).Add(1)
			}()
			time.Sleep(p.interval)
		}
	}
}

func (p *Pipeline) Cleanup(g *sync.WaitGroup) {
	p.publisher.Cleanup(g)
}
