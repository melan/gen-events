package events_generator

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"github.com/melan/gen-events/misc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	case1NumberOfLongDown = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case1_long_down_devices",
		},
		[]string{"orgId"})
	case1ReturnFromLongDown = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case1_returned_long_down_devices",
		},
		[]string{"orgId"})
	case1EnteredFromLongDown = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case1_entered_long_down_devices",
		},
		[]string{"orgId"})
	case1ShortDown = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case1_short_down_devices",
		},
		[]string{"orgId"})
	case1LateDown = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case1_late_down_devices",
		},
		[]string{"orgId"})
	case1LateUp = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case1_late_up_devices",
		},
		[]string{"orgId"})
	case1Up = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case1_up_devices",
		},
		[]string{"orgId"})
)

type heartbeatMessage struct {
	deviceMessage
	Status string `json:"status"`
}

func (hbm *heartbeatMessage) ToJson() ([]byte, error) {
	return json.Marshal(hbm)
}

type case1Device struct {
	OrgId               string  `json:"org_id"`
	DeviceId            int     `json:"deviceId"`
	ProbabilityDown     float64 `json:"probabilityDown"`
	ProbabilityLongDown float64 `json:"probabilityLongDown"`
	LastUp              int64   `json:"lastUp"`
	IsLongDown          bool    `json:"isLongDown"`
	Quality             float64 `json:"quality"`
	DebugEvents         bool    `json:"debug_events"`
}

func generateCase1Devices(orgId string, n int, stdDev float64, debugEvents bool) []device {
	randomSource := rand.NewSource(time.Now().UnixNano())
	r := rand.New(randomSource)

	devices := make([]device, 0, n)
	downThreshold := stdDev * 1.5     // approx 1 in 7 devices or 14%
	longDownThreshold := stdDev * 3.0 // approx 1 in 370 devices or 0.3%

	defaultDownProbability := 0.1
	defaultDownProbabilityLong := 0.01

	for i := 0; i < n; i++ {
		deviceQuality := math.Abs(r.NormFloat64()) * stdDev

		downProbability := defaultDownProbability
		downProbabilityLong := defaultDownProbabilityLong

		if deviceQuality >= downThreshold {
			downProbability = .6
		}

		if deviceQuality >= longDownThreshold {
			downProbabilityLong = .6
		}

		device := &case1Device{
			OrgId:               orgId,
			DeviceId:            i,
			ProbabilityDown:     downProbability,
			ProbabilityLongDown: downProbabilityLong,
			LastUp:              -1,
			IsLongDown:          false,
			Quality:             deviceQuality,
			DebugEvents:         debugEvents,
		}
		log.Printf("Device %s_%s/%d: %v", CaseOne, orgId, i, device)

		devices = append(devices, device)
	}

	return devices
}
func (cod *case1Device) String() string {
	return fmt.Sprintf("org: %s, deviceId: %d, quality: %.02f, probDown: %.02f, probLongDown: %.02f, lastUp: %d, isLongDown: %t",
		cod.OrgId, cod.DeviceId, cod.Quality, cod.ProbabilityDown, cod.ProbabilityLongDown, cod.LastUp, cod.IsLongDown)
}

func (cod *case1Device) Generate() Event {
	now := time.Now().Unix()

	if cod.LastUp == -1 {
		if cod.DebugEvents {
			log.Printf("%d: d %s/%d first event", now, cod.OrgId, cod.DeviceId)
		}
		cod.LastUp = now
		case1Up.WithLabelValues(cod.OrgId).Inc()
		return &heartbeatMessage{
			deviceMessage: deviceMessage{
				DeviceId: cod.DeviceId,
				Time:     now,
			},
			Status: "UP",
		}
	}

	if cod.IsLongDown && (now-cod.LastUp) <= 20*60 { // 20 minutes
		if cod.DebugEvents {
			log.Printf("%d: d %s/%d is long down", now, cod.OrgId, cod.DeviceId)
		}
		case1NumberOfLongDown.WithLabelValues(cod.OrgId).Inc()
		return nil
	} else if cod.IsLongDown {
		cod.IsLongDown = false
		cod.LastUp = now
		if cod.DebugEvents {
			log.Printf("%d: d %s/%d returns from long down", now, cod.OrgId, cod.DeviceId)
		}
		case1ReturnFromLongDown.WithLabelValues(cod.OrgId).Inc()
		return &heartbeatMessage{
			deviceMessage: deviceMessage{
				DeviceId: cod.DeviceId,
				Time:     now,
			},
			Status: "UP",
		}
	}

	cod.LastUp = now

	if chance := rand.Float64(); chance < .01 { // send late message
		newNow := now - (10+rand.Int63n(10))*60
		if chance < .005 {
			if cod.DebugEvents {
				log.Printf("%d: d %s/%d is late and %s", newNow, cod.OrgId, cod.DeviceId, "UP")
			}
			case1LateUp.WithLabelValues(cod.OrgId).Inc()

			return &heartbeatMessage{
				deviceMessage: deviceMessage{
					DeviceId: cod.DeviceId,
					Time:     newNow, // send the event back to 10-20 minutes
				},
				Status: "UP",
			}
		} else {
			if cod.DebugEvents {
				log.Printf("%d: d %s/%d is late and %s", newNow, cod.OrgId, cod.DeviceId, "DOWN")
			}
			case1LateDown.WithLabelValues(cod.OrgId).Inc()
			return nil
		}
	}

	if rand.Float64() < cod.ProbabilityDown { // going short down
		if cod.DebugEvents {
			log.Printf("%d: d %s/%d short down", now, cod.OrgId, cod.DeviceId)
		}
		case1ShortDown.WithLabelValues(cod.OrgId).Inc()
		return nil
	}

	if rand.Float64() < cod.ProbabilityLongDown { // going long down
		if cod.DebugEvents {
			log.Printf("%d: d %s/%d going long down", now, cod.OrgId, cod.DeviceId)
		}
		case1EnteredFromLongDown.WithLabelValues(cod.OrgId).Inc()
		cod.IsLongDown = true
		return nil
	}

	if cod.DebugEvents {
		log.Printf("%d: d %s/%d is UP", now, cod.OrgId, cod.DeviceId)
	}
	case1Up.WithLabelValues(cod.OrgId).Inc()
	return &heartbeatMessage{
		deviceMessage: deviceMessage{
			DeviceId: cod.DeviceId,
			Time:     now,
		},
		Status: "UP",
	}
}
