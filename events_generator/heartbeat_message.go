package events_generator

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"time"
)

type HeartbeatMessage struct {
	DeviceId int    `json:"device_id"`
	Time     int64  `json:"time"`
	Status   string `json:"status"`
}

func (hbm *HeartbeatMessage) ToJson() ([]byte, error) {
	return json.Marshal(hbm)
}

func (hbm *HeartbeatMessage) PartitionKey() string {
	return strconv.Itoa(hbm.DeviceId)
}

type case1Device struct {
	DeviceId            int     `json:"deviceId"`
	ProbabilityDown     float64 `json:"probabilityDown"`
	ProbabilityLongDown float64 `json:"probabilityLongDown"`
	LastUp              int64   `json:"lastUp"`
	IsLongDown          bool    `json:"isLongDown"`
	Quality             float64 `json:"quality"`
	DebugEvents         bool    `json:"debug_events"`
}

func (cod *case1Device) String() string {
	return fmt.Sprintf("deviceId: %d, quality: %.02f, probDown: %.02f, probLongDown: %.02f, lastUp: %d, isLongDown: %t",
		cod.DeviceId, cod.Quality, cod.ProbabilityDown, cod.ProbabilityLongDown, cod.LastUp, cod.IsLongDown)
}

func (cod *case1Device) generate() Event {
	now := time.Now().Unix()

	if cod.LastUp == -1 {
		if cod.DebugEvents {
			log.Printf("%d: d %d first event", now, cod.DeviceId)
		}
		cod.LastUp = now
		return &HeartbeatMessage{
			Time:     now,
			DeviceId: cod.DeviceId,
			Status:   "UP",
		}
	}

	if cod.IsLongDown && (now-cod.LastUp) <= 20*60 { // 20 minutes
		if cod.DebugEvents {
			log.Printf("%d: d %d is long down", now, cod.DeviceId)
		}
		return nil
	} else if cod.IsLongDown {
		cod.IsLongDown = false
		cod.LastUp = now
		if cod.DebugEvents {
			log.Printf("%d: d %d returns from long down", now, cod.DeviceId)
		}
		return &HeartbeatMessage{
			Time:     now,
			DeviceId: cod.DeviceId,
			Status:   "UP",
		}
	}

	cod.LastUp = now

	if chance := rand.Float64(); chance < .01 { // send late message
		var status string
		if chance < .005 {
			status = "UP"
		} else {
			status = "DOWN"
		}

		newNow := now - (10+rand.Int63n(10))*60
		if cod.DebugEvents {
			log.Printf("%d: d %d is late and %s", newNow, cod.DeviceId, status)
		}

		return &HeartbeatMessage{
			Time:     newNow, // send the event back to 10-20 minutes
			DeviceId: cod.DeviceId,
			Status:   status,
		}
	}

	if rand.Float64() < cod.ProbabilityDown { // going short down
		if cod.DebugEvents {
			log.Printf("%d: d %d short down", now, cod.DeviceId)
		}
		return &HeartbeatMessage{
			Time:     now,
			DeviceId: cod.DeviceId,
			Status:   "DOWN",
		}
	}

	if rand.Float64() < cod.ProbabilityLongDown { // going long down
		if cod.DebugEvents {
			log.Printf("%d: d %d going long down", now, cod.DeviceId)
		}
		cod.IsLongDown = true
		return nil
	}

	if cod.DebugEvents {
		log.Printf("%d: d %d is UP", now, cod.DeviceId)
	}
	return &HeartbeatMessage{
		Time:     now,
		DeviceId: cod.DeviceId,
		Status:   "UP",
	}
}

func generateCase1Devices(n int, stdDev float64, debugEvents bool) []device {
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
			DeviceId:            i,
			ProbabilityDown:     downProbability,
			ProbabilityLongDown: downProbabilityLong,
			LastUp:              -1,
			IsLongDown:          false,
			Quality:             deviceQuality,
			DebugEvents:         debugEvents,
		}
		log.Printf("Device %d: %v", i, device)

		devices = append(devices, device)
	}

	return devices
}
