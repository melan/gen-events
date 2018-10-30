package events_generator

import (
	"math/rand"
	"strconv"
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
	CaseOne   Case = "1"
	CaseTwo   Case = "2"
	CaseThree Case = "3"
	CaseFour  Case = "4"
	CaseFive  Case = "5"
)

type Org struct {
	OrgId         string
	OrgSize       OrgSize
	CaseId        Case
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
	}

	return 0
}

func GenerateOrg(id string, size OrgSize, caseId Case, debugEvents bool) *Org {
	var devices []device
	var kinesisPrefix string

	switch caseId {
	case CaseOne:
		devices = generateCase1Devices(getNumberOfDevices(size), 1, debugEvents)
		kinesisPrefix = "heartbeat_message"
	case CaseTwo:
		devices = generateCase2Devices(getNumberOfDevices(size), debugEvents)
		kinesisPrefix = "structured_error_message"
	case CaseThree:
		devices = generateCase3Devices(getNumberOfDevices(size), debugEvents)
		kinesisPrefix = "temperature_reading"
	case CaseFour:
		devices = generateCase3Devices(getNumberOfDevices(size), debugEvents)
		kinesisPrefix = "temperature_reading"
	case CaseFive:
		devices = make([]device, 0)
		kinesisPrefix = "data_change"
	default:
		devices = make([]device, 0)
		kinesisPrefix = "unknown"
	}

	return &Org{
		OrgId:         id,
		OrgSize:       size,
		CaseId:        caseId,
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

	return events
}

func (org *Org) StreamName() string {
	return org.KinesisPrefix + "_" + org.OrgId
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
