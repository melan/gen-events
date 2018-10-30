package events_generator

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/melan/gen-events/misc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	case2LongError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case2_long_error_devices",
		},
		[]string{"orgId"})
	case2NewError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case2_new_error_devices",
		},
		[]string{"orgId"})
	case2NewLongError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case2_new_long_error_devices",
		},
		[]string{"orgId"})
	case2NewShortError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case2_new_short_error_devices",
		},
		[]string{"orgId"})
	case2SameError = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case2_same_error_devices",
		},
		[]string{"orgId"})
)

type case2Error string

const (
	ContinueError case2Error = "CONTINUE"
	SuccessError  case2Error = "SUCCESS"
	RedirectError case2Error = "REDIRECT"
	UserError     case2Error = "USER"
	SystemError   case2Error = "SYSTEM"
)

var (
	allCase2Errors = []case2Error{ContinueError, SuccessError, RedirectError, UserError, SystemError}
	TwoKError      = loremIpsum[:2048]
)

type noisyErrorMessage struct {
	deviceMessage
	ErrorType    case2Error `json:"error_type"`
	ErrorMessage string     `json:"error_message"`
}

func (nem *noisyErrorMessage) ToJson() ([]byte, error) {
	return json.Marshal(nem)
}

type case2Device struct {
	OrgId                string
	DeviceId             int
	ProbabilityNewError  float64
	ProbabilityLongError float64
	LastError            case2Error
	LastErrorChange      int64
	IsLongError          bool
	DebugEvents          bool
}

func generateCase2Devices(orgId string, n int, debugEvents bool) []device {
	devices := make([]device, 0, n)
	for i := 0; i < n; i++ {
		device := &case2Device{
			OrgId:                orgId,
			DeviceId:             i,
			ProbabilityNewError:  0.1,
			ProbabilityLongError: 0.03,
			LastError:            allCase2Errors[rand.Intn(len(allCase2Errors))],
			LastErrorChange:      -1,
			IsLongError:          false,
			DebugEvents:          debugEvents,
		}
		log.Printf("Device: %s/%d: %v", orgId, i, device)

		devices = append(devices, device)
	}

	return devices
}

func (ctd *case2Device) String() string {
	return fmt.Sprintf("org: %s, deviceId: %d, probLongEror: %.02f, lastError: %s, lastErrChange: %d, debugEvents: %t",
		ctd.OrgId, ctd.DeviceId, ctd.ProbabilityLongError, ctd.LastError, ctd.LastErrorChange, ctd.DebugEvents)
}

func (ctd *case2Device) Generate() Event {
	now := time.Now().Unix()

	if ctd.IsLongError && (now-ctd.LastErrorChange) <= 7*60 { // keep long error for 7 minutes
		if ctd.DebugEvents {
			log.Printf("%d: d %s/%d is in long error: %s", now, ctd.OrgId, ctd.DeviceId, ctd.LastError)
		}
		case2LongError.WithLabelValues(ctd.OrgId).Inc()
		return &noisyErrorMessage{
			deviceMessage: deviceMessage{
				DeviceId: ctd.DeviceId,
				Time:     now,
			},
			ErrorType:    ctd.LastError,
			ErrorMessage: generateErrorMessage(),
		}
	}

	if rand.Float64() < ctd.ProbabilityNewError {
		// switch to a new error
		case2NewError.WithLabelValues(ctd.OrgId).Inc()

		// get list of possible candidates
		newErrors := make([]case2Error, 0, len(allCase2Errors)-1)
		for _, c2e := range allCase2Errors {
			if c2e == ctd.LastError {
				continue
			}

			newErrors = append(newErrors, c2e)
		}

		ctd.LastErrorChange = now
		ctd.LastError = newErrors[rand.Intn(len(newErrors))]

		if ctd.DebugEvents {
			log.Printf("%d: d %s/%d new error is %s", now, ctd.OrgId, ctd.DeviceId, ctd.LastError)
		}

		if rand.Float64() < ctd.ProbabilityLongError {
			if ctd.DebugEvents {
				log.Printf("%d: d %s/%d goes into long error cycle", now, ctd.OrgId, ctd.DeviceId)
			}
			case2NewLongError.WithLabelValues(ctd.OrgId).Inc()
			ctd.IsLongError = true
		} else {
			if ctd.DebugEvents {
				log.Printf("%d: d %s/%d goes into short error mode", now, ctd.OrgId, ctd.DeviceId)
			}
			case2NewShortError.WithLabelValues(ctd.OrgId).Inc()
			ctd.IsLongError = false
		}

		return &noisyErrorMessage{
			deviceMessage: deviceMessage{
				DeviceId: ctd.DeviceId,
				Time:     now,
			},
			ErrorType:    ctd.LastError,
			ErrorMessage: generateErrorMessage(),
		}
	}

	if ctd.DebugEvents {
		log.Printf("%d: d %s/%d remains at error %s. No message", now, ctd.OrgId, ctd.DeviceId, ctd.LastError)
	}
	case2SameError.WithLabelValues(ctd.OrgId).Inc()
	return nil
}

func generateErrorMessage() string {
	test := rand.Float64()
	if test < .8 { // 80% it should return 2KB message
		return TwoKError
	} else if test >= .8 && test < .85 { // 5% is should return 5KB message
		return loremIpsum
	} else { // 15% is should return a message of a random size between 0 and 1KB
		errorMessage := loremIpsumWords[:rand.Intn(len(loremIpsumWords)/5)]
		return strings.Join(errorMessage, " ")
	}
}
