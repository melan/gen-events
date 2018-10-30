package events_generator

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/melan/gen-events/misc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	case4BrokenDevice = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case4_broken_devices",
		},
		[]string{"orgId"})
	case4RestoredDevice = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case4_restored_devices",
		},
		[]string{"orgId"})
)

type case4Device struct {
	case34Device
	IsBroken bool  `json:"is_broken"`
	LastUp   int64 `json:"last_up"`
}

func generateCase4Devices(orgId string, n int, debugEvents bool) []device {
	now := time.Now().Unix()
	c3Devices := generateCase34Devices(CaseFour, orgId, n, debugEvents)
	devices := make([]device, 0, n)

	for _, d := range c3Devices {
		c4d := &case4Device{
			case34Device: *d,
			IsBroken:     false,
			LastUp:       now,
		}

		devices = append(devices, c4d)
	}

	return devices
}

func (d *case4Device) String() string {
	return fmt.Sprintf("%s, broken: %t, lastUp: %d",
		d.case34Device.String(), d.IsBroken, d.LastUp)
}

func (d *case4Device) Generate() Event {
	now := time.Now().Unix()

	if d.IsBroken && (now-d.LastUp) < 6*60 { // device is broken still - it's down for 6 minutes
		if d.DebugEvents {
			log.Printf("%d: d %s/%d is broken", now, d.OrgId, d.DeviceId)
		}
		case4BrokenDevice.WithLabelValues(d.OrgId).Inc()
		return nil
	} else if d.IsBroken {
		if d.DebugEvents {
			log.Printf("%d: d %s/%d is back", now, d.OrgId, d.DeviceId)
		}
		case4RestoredDevice.WithLabelValues(d.OrgId).Inc()
		d.IsBroken = false
		d.LastUp = now
		return d.case34Device.Generate()
	}

	if rand.Float32() < .138 { // break the device
		if d.DebugEvents {
			log.Printf("%d: d %s/%d is breaking", now, d.OrgId, d.DeviceId)
		}
		d.IsBroken = true
		case4BrokenDevice.WithLabelValues(d.OrgId).Inc()
		return nil
	} else {
		d.LastUp = now
		return d.case34Device.Generate()
	}
}
