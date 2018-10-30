package events_generator

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

type case4Device struct {
	case3Device
	IsBroken bool  `json:"is_broken"`
	LastUp   int64 `json:"last_up"`
}

func generateCase4Devices(n int, debugEvents bool) []device {
	now := time.Now().Unix()
	c3Devices := generateCase34Devices(n, debugEvents)
	devices := make([]device, 0, n)

	for _, d := range c3Devices {
		c4d := &case4Device{
			case3Device: *d,
			IsBroken:    false,
			LastUp:      now,
		}

		devices = append(devices, c4d)
	}

	return devices
}

func (d *case4Device) String() string {
	return fmt.Sprintf("%s, broken: %t, lastUp: %d",
		d.case3Device.String(), d.IsBroken, d.LastUp)
}

func (d *case4Device) Generate() Event {
	now := time.Now().Unix()

	if d.IsBroken && (now-d.LastUp) < 6*60 { // device is broken still - it's down for 6 minutes
		if d.DebugEvents {
			log.Printf("%d: d %d is broken", now, d.DeviceId)
		}
		return nil
	} else if d.IsBroken {
		d.IsBroken = false
		if d.DebugEvents {
			log.Printf("%d: d %d is back", now, d.DeviceId)
		}
	}

	if rand.Float32() < .138 { // break the device
		if d.DebugEvents {
			log.Printf("%d: d %d is breaking", now, d.DeviceId)
		}
		d.IsBroken = true
		return nil
	} else {
		d.LastUp = now
		return d.case3Device.Generate()
	}
}
