package events_generator

import (
	"math/rand"
	"strconv"

	"github.com/melan/gen-events/misc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	numberOfDevices = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "number_of_devices",
			Help:      "Number of devices generated for the use case in the org",
		},
		[]string{"orgId", "caseId"})

	numberOfEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "generated_events",
		},
		[]string{"orgId", "caseId"})
)

type Event interface {
	PartitionKey() string
	ToJson() ([]byte, error)
}

type device interface {
	Generate() Event
	String() string
}

type deviceMessage struct {
	DeviceId int   `json:"device_id"`
	Time     int64 `json:"time"`
}

func (m *deviceMessage) PartitionKey() string {
	return strconv.Itoa(m.DeviceId)
}

type OrgSize string

const (
	TinyOrg   OrgSize = "tiny"
	SmallOrg  OrgSize = "small"
	MediumOrg OrgSize = "medium"
	LargeOrg  OrgSize = "large"
)

type Case string

const (
	CaseOne   Case = "heartbeat_message"
	CaseTwo   Case = "structured_error_message"
	CaseThree Case = "temperature_reading"
	CaseFour  Case = "broken_temperature_reading"
	CaseFive  Case = "data_change"
)

type Org struct {
	OrgId         string
	OrgSize       OrgSize
	CaseId        Case
	GlobalPrefix  string
	KinesisPrefix string
	Devices       []device
	DebugEvents   bool
}

func getNumberOfDevices(orgSize OrgSize) int {
	switch orgSize {
	case TinyOrg:
		return 10
	case SmallOrg:
		return 7500
	case MediumOrg:
		return 75000
	case LargeOrg:
		return 1e6
	default:
		return getNumberOfDevices(TinyOrg)
	}

	return 0
}

func GenerateOrg(id string, size OrgSize, caseId Case, debugEvents bool, prefix string) *Org {
	var devices []device
	var kinesisPrefix string

	switch caseId {
	case CaseOne:
		devices = generateCase1Devices(id, getNumberOfDevices(size), 1, debugEvents)
		kinesisPrefix = "heartbeat_message"
	case CaseTwo:
		devices = generateCase2Devices(id, getNumberOfDevices(size), debugEvents)
		kinesisPrefix = "structured_error_message"
	case CaseThree:
		devices = generateCase3Devices(id, getNumberOfDevices(size), debugEvents)
		kinesisPrefix = "temperature_reading"
	case CaseFour:
		devices = generateCase4Devices(id, getNumberOfDevices(size), debugEvents)
		kinesisPrefix = "broken_temperature_reading"
	case CaseFive:
		devices = generateCase5(id, getNumberOfDevices(size), debugEvents)
		kinesisPrefix = "data_change"
	default:
		devices = make([]device, 0)
		kinesisPrefix = "unknown"
	}

	numberOfDevices.WithLabelValues(id, string(caseId)).Set(float64(len(devices)))

	return &Org{
		OrgId:         id,
		OrgSize:       size,
		CaseId:        caseId,
		GlobalPrefix:  prefix,
		KinesisPrefix: kinesisPrefix,
		Devices:       devices,
		DebugEvents:   debugEvents,
	}
}

func (org *Org) GenerateEvents() []Event {
	events := make([]Event, 0, len(org.Devices))

	for _, d := range org.Devices {
		if event := d.Generate(); event != nil {
			events = append(events, event)
		}
	}

	numberOfEvents.WithLabelValues(org.OrgId, string(org.CaseId)).Add(float64(len(events)))

	return events
}

func (org *Org) StreamName() string {
	return org.GlobalPrefix + "_" + org.KinesisPrefix + "_" + org.OrgId
}

func (org *Org) NumberOfStreamShards() int64 {
	switch org.OrgSize {
	case TinyOrg:
		return 1
	case SmallOrg:
		return 1
	case MediumOrg:
		return 2
	case LargeOrg:
		return 13
	default:
		return 1
	}
}

func GuessOrgSize() OrgSize {
	guess := rand.Float64()
	/**
			* Large - 1%
	        * Medium - 10%
	        * Small - 80%
	        * Tiny - 9%
	*/

	if guess <= .01 {
		return LargeOrg
	}
	if guess > 0.01 && guess < .1 {
		return TinyOrg
	}
	if guess >= .1 && guess < .2 {
		return MediumOrg
	}

	return SmallOrg
}
