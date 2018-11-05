package events_generator

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"time"
	"unicode"

	"github.com/melan/gen-events/misc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	case34LongSpikeDevice = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case34_long_spike_devices",
		},
		[]string{"orgId", "caseName"})
	case34EnterLongSpikeDevice = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case34_enter_long_spike_devices",
		},
		[]string{"orgId", "caseName"})
	case34ReturnLongSpikeDevice = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case34_return_from_long_spike_devices",
		},
		[]string{"orgId", "caseName"})
	case34EnterShortSpikeDevice = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case34_enter_short_spike_devices",
		},
		[]string{"orgId", "caseName"})
	case34NormalLevelDevice = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case34_normal_level_devices",
		},
		[]string{"orgId", "caseName"})
	case34LateMessage = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case34_late_message",
		},
		[]string{"orgId", "caseName"})
)

type temperatureReadingMessage struct {
	deviceMessage
	DeviceName  string `json:"device_name"`
	Temperature int    `json:"temp"`
}

func (m *temperatureReadingMessage) ToJson() ([]byte, error) {
	return json.Marshal(m)
}

type case34Device struct {
	OrgId              string `json:"org_id"`
	DeviceId           int    `json:"device_id"`
	DeviceName         string `json:"device_name"`
	LastTemperature    int    `json:"last_temperature"`
	SumTemperature     int64  `json:"sum_temperature"`
	CountMeasurements  int    `json:"count_measurements"`
	IsInLongSpike      bool   `json:"is_in_long_spike"`
	StepsLeftLongSpike int    `json:"steps_left_long_spike"`
	DebugEvents        bool   `json:"debug_events"`
	Case               Case   `json:"case"`
}

func generateCase34Devices(caseName Case, orgId string, n int, debugEvents bool) []*case34Device {
	randomSource := rand.NewSource(time.Now().UnixNano())
	r := rand.New(randomSource)

	devices := make([]*case34Device, 0, n)

	for i := 0; i < n; i++ {
		temp := r.Intn(121) - 10
		deviceWord := loremIpsumWords[r.Intn(len(loremIpsumWords))]
		deviceWord = strings.TrimFunc(deviceWord, unicode.IsPunct)
		device := &case34Device{
			OrgId:              orgId,
			DeviceId:           i,
			DeviceName:         fmt.Sprintf("device_%s_%d", deviceWord, i),
			LastTemperature:    temp,
			SumTemperature:     int64(temp),
			CountMeasurements:  1,
			IsInLongSpike:      false,
			StepsLeftLongSpike: 0,
			DebugEvents:        debugEvents,
			Case:               caseName,
		}
		log.Printf("Device %s_%s/%d: %v", caseName, orgId, i, device)

		devices = append(devices, device)
	}

	return devices
}

func generateCase3Devices(orgId string, n int, debugEvents bool) []device {
	case3Devices := generateCase34Devices(CaseThree, orgId, n, debugEvents)
	devices := make([]device, 0, n)

	for _, d := range case3Devices {
		devices = append(devices, d)
	}

	return devices
}

func (d *case34Device) String() string {
	return fmt.Sprintf("orgId: %s, deviceId: %d, deviceName: %s, meanTemp: %.02f, lastTemp: %d, debugEvents: %t",
		d.OrgId, d.DeviceId, d.DeviceName, float64(d.SumTemperature)/float64(d.CountMeasurements), d.LastTemperature, d.DebugEvents)
}

func (d *case34Device) Generate() Event {
	mean := float64(d.SumTemperature) / float64(d.CountMeasurements)
	now := time.Now().Unix()

	if d.IsInLongSpike && d.StepsLeftLongSpike > 0 {
		// proceed with long spike
		if float64(d.LastTemperature) > mean {
			d.LastTemperature++
			if d.DebugEvents {
				log.Printf("%d: d %s/%d is in long spike and moving up. Mean: %.02f, Current: %d, Steps left: %d",
					now, d.OrgId, d.DeviceId, mean, d.LastTemperature, d.StepsLeftLongSpike-1)
			}
		} else {
			d.LastTemperature--
			if d.DebugEvents {
				log.Printf("%d: d %s/%d is in long spike and moving down. Mean: %.02f, Current: %d, Steps left: %d",
					now, d.OrgId, d.DeviceId, mean, d.LastTemperature, d.StepsLeftLongSpike-1)
			}
		}
		case34LongSpikeDevice.WithLabelValues(d.OrgId, string(d.Case)).Inc()
		d.StepsLeftLongSpike--
	}

	if !d.IsInLongSpike && rand.Float32() < .03 {
		// begin a spike
		var direction int
		if rand.Float32() >= .5 { //
			direction = -1
		} else {
			direction = 1
		}

		// decide if it's a long spike
		if rand.Float32() < .1 {
			d.StepsLeftLongSpike = 5 + rand.Intn(5)
			d.IsInLongSpike = true
			d.LastTemperature = d.LastTemperature + direction
			if d.DebugEvents {
				log.Printf("%d: d %s/%d enters long spike. Direction: %d. Mean: %.02f, Current: %d, Steps left: %d",
					now, d.OrgId, d.DeviceId, direction, mean, d.LastTemperature, d.StepsLeftLongSpike)
			}
			case34EnterLongSpikeDevice.WithLabelValues(d.OrgId, string(d.Case)).Inc()
		} else {
			d.LastTemperature = int(mean) + direction*(6+rand.Intn(5))
			if d.DebugEvents {
				log.Printf("%d: d %s/%d enters short spike. Direction: %d. Mean: %.02f, Current: %d",
					now, d.OrgId, d.DeviceId, direction, mean, d.LastTemperature)
			}
			case34EnterShortSpikeDevice.WithLabelValues(d.OrgId, string(d.Case)).Inc()
		}
	} else if !(d.IsInLongSpike && d.StepsLeftLongSpike > 0) {
		if d.IsInLongSpike && d.StepsLeftLongSpike == 0 {
			// fall back to normal
			d.IsInLongSpike = false
			if d.DebugEvents {
				log.Printf("%d: d %s/%d returns from long spike", now, d.OrgId, d.DeviceId)
			}
			case34ReturnLongSpikeDevice.WithLabelValues(d.OrgId, string(d.Case)).Inc()
		}

		// generate a new value around mean
		d.LastTemperature = int(math.Round(rand.NormFloat64()*.5 + mean))
		if d.DebugEvents {
			log.Printf("%d: d %s/%d in normal mode. Mean: %.02f, Current: %d, Delta: %.02f",
				now, d.OrgId, d.DeviceId, mean, d.LastTemperature, float64(d.LastTemperature)-mean)
		}
		case34NormalLevelDevice.WithLabelValues(d.OrgId, string(d.Case)).Inc()
	}

	if rand.Float32() < .01 { // send late message
		now = now - (10+rand.Int63n(10))*60
		if d.DebugEvents {
			log.Printf("%d: d %s/%d late message", now, d.OrgId, d.DeviceId)
		}
		case34LateMessage.WithLabelValues(d.OrgId, string(d.Case)).Inc()
	}

	d.SumTemperature += int64(d.LastTemperature)
	d.CountMeasurements++

	return &temperatureReadingMessage{
		deviceMessage: deviceMessage{
			DeviceId: d.DeviceId,
			Time:     now,
		},
		DeviceName:  d.DeviceName,
		Temperature: d.LastTemperature,
	}
}
