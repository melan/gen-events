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
)

type temperatureReadingMessage struct {
	deviceMessage
	DeviceName  string `json:"device_name"`
	Temperature int    `json:"temp"`
}

func (m *temperatureReadingMessage) ToJson() ([]byte, error) {
	return json.Marshal(m)
}

type case3Device struct {
	DeviceId           int    `json:"device_id"`
	DeviceName         string `json:"device_name"`
	LastTemperature    int    `json:"last_temperature"`
	SumTemperature     int64  `json:"sum_temperature"`
	CountMeasurements  int    `json:"count_measurements"`
	IsInLongSpike      bool   `json:"is_in_long_spike"`
	StepsLeftLongSpike int    `json:"steps_left_long_spike"`
	DebugEvents        bool   `json:"debug_events"`
}

func generateCase34Devices(n int, debugEvents bool) []*case3Device {
	randomSource := rand.NewSource(time.Now().UnixNano())
	r := rand.New(randomSource)

	devices := make([]*case3Device, 0, n)

	for i := 0; i < n; i++ {
		temp := r.Intn(121) - 10
		deviceWord := loremIpsumWords[r.Intn(len(loremIpsumWords))]
		deviceWord = strings.TrimFunc(deviceWord, unicode.IsPunct)
		device := &case3Device{
			DeviceId:           i,
			DeviceName:         fmt.Sprintf("device_%s_%d", deviceWord, i),
			LastTemperature:    temp,
			SumTemperature:     int64(temp),
			CountMeasurements:  1,
			IsInLongSpike:      false,
			StepsLeftLongSpike: 0,
			DebugEvents:        debugEvents,
		}
		log.Printf("Device %d: %v", i, device)

		devices = append(devices, device)
	}

	return devices
}

func generateCase3Devices(n int, debugEvents bool) []device {
	case3Devices := generateCase34Devices(n, debugEvents)
	devices := make([]device, 0, n)

	for _, d := range case3Devices {
		devices = append(devices, d)
	}

	return devices
}

func (d *case3Device) String() string {
	return fmt.Sprintf("deviceId: %d, deviceName: %s, meanTemp: %.02f, lastTemp: %d, debugEvents: %t",
		d.DeviceId, d.DeviceName, float64(d.SumTemperature)/float64(d.CountMeasurements), d.LastTemperature, d.DebugEvents)
}

func (d *case3Device) Generate() Event {
	mean := float64(d.SumTemperature) / float64(d.CountMeasurements)
	now := time.Now().Unix()

	if d.IsInLongSpike && d.StepsLeftLongSpike > 0 {
		// proceed with long spike
		if float64(d.LastTemperature) > mean {
			d.LastTemperature++
			if d.DebugEvents {
				log.Printf("%d: d %d is in long spike and moving up. Mean: %.02f, Current: %d, Steps left: %d",
					now, d.DeviceId, mean, d.LastTemperature, d.StepsLeftLongSpike-1)
			}
		} else {
			d.LastTemperature--
			if d.DebugEvents {
				log.Printf("%d: d %d is in long spike and moving down. Mean: %.02f, Current: %d, Steps left: %d",
					now, d.DeviceId, mean, d.LastTemperature, d.StepsLeftLongSpike-1)
			}
		}
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
				log.Printf("%d: d %d enters long spike. Direction: %d. Mean: %.02f, Current: %d, Steps left: %d",
					now, d.DeviceId, direction, mean, d.LastTemperature, d.StepsLeftLongSpike)
			}
		} else {
			d.LastTemperature = int(mean) + direction*(6+rand.Intn(5))
			if d.DebugEvents {
				log.Printf("%d: d %d enters short spike. Direction: %d. Mean: %.02f, Current: %d",
					now, d.DeviceId, direction, mean, d.LastTemperature)
			}
		}
	} else if !(d.IsInLongSpike && d.StepsLeftLongSpike > 0) {
		if d.IsInLongSpike && d.StepsLeftLongSpike == 0 {
			// fall back to normal
			d.IsInLongSpike = false
		}

		// generate a new value around mean
		d.LastTemperature = int(math.Round(rand.NormFloat64()*.5 + mean))
		if d.DebugEvents {
			log.Printf("%d: d %d in normal mode. Mean: %.02f, Current: %d, Delta: %.02f",
				now, d.DeviceId, mean, d.LastTemperature, float64(d.LastTemperature)-mean)
		}
	}

	if rand.Float32() < .01 { // send late message
		now = now - (10+rand.Int63n(10))*60
		log.Printf("%d: d %d late message", now, d.DeviceId)
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
