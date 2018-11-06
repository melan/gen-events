package events_generator

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/melan/gen-events/misc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

var (
	case5CurrentMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case5_current_messages",
		},
		[]string{"orgId"})
	case5PastMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case5_past_messages",
		},
		[]string{"orgId"})
	case5SkippedMessages = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: misc.MetricsPrefix,
			Name:      "case5_skipped_messages",
		},
		[]string{"orgId"})
)

type randomChangeMessage struct {
	Id             string  `json:"id"`
	FirstName      string  `json:"first_name"`
	LastName       string  `json:"last_name"`
	ChangeDate     int64   `json:"change_date"`
	CustomerRating float32 `json:"customer_rating"`
}

func (m *randomChangeMessage) ToJson() ([]byte, error) {
	return json.Marshal(m)
}

func (m *randomChangeMessage) PartitionKey() string {
	return m.Id
}

type case5 struct {
	OrgId          string  `json:"org_id"`
	Id             string  `json:"id"`
	FirstName      string  `json:"first_name"`
	LastName       string  `json:"last_name"`
	LastChangeData int64   `json:"last_change_data"`
	CurrentRating  float64 `json:"current_rating"`
	DebugEvents    bool    `json:"debug_events"`
}

func generateCase5(orgId string, n int, debugEvents bool) []device {
	now := time.Now().Unix()
	actualNumber := int(float32(n) / .036)
	devices := make([]device, 0, actualNumber)

	for i := 0; i < actualNumber; i++ {
		firstName := getName()
		lastName := getName()

		device := &case5{
			OrgId:          orgId,
			Id:             strconv.Itoa(i),
			FirstName:      firstName,
			LastName:       lastName,
			CurrentRating:  float64(rand.Intn(10)),
			LastChangeData: now,
			DebugEvents:    debugEvents,
		}

		log.Printf("Device %s_%s/%d: %v", CaseFive, orgId, i, device)
		devices = append(devices, device)
	}
	return devices
}

func (c *case5) String() string {
	return fmt.Sprintf("orgid: %s, id: %s, fn: %s, ln: %s, rating: %.02f",
		c.OrgId, c.Id, c.FirstName, c.LastName, c.CurrentRating)
}

func (c *case5) Generate() Event {
	now := time.Now().Unix()

	// 3.6% of contacts should send messages
	if rand.Float32() < .036 {
		newRating := c.CurrentRating + rand.NormFloat64()
		if rand.Float32() < .77 { // 77% of the sent messages are from the present
			// send message from present
			if c.DebugEvents {
				log.Printf("%d: c %s/%s sends update from present", now, c.OrgId, c.Id)
			}
			c.LastChangeData = now
			c.CurrentRating = newRating
			case5CurrentMessages.WithLabelValues(c.OrgId).Inc()
		} else {
			// send message from past
			now = c.LastChangeData - rand.Int63n(1000)
			if c.DebugEvents {
				log.Printf("%d: c %s/%s sends update from past", now, c.OrgId, c.Id)
			}
			case5PastMessages.WithLabelValues(c.OrgId).Inc()
		}

		return &randomChangeMessage{
			Id:             c.Id,
			FirstName:      c.FirstName,
			LastName:       c.LastName,
			CustomerRating: float32(newRating),
			ChangeDate:     now,
		}
	} else {
		if c.DebugEvents {
			log.Printf("%d: c %s/%s has no updates. skipping", now, c.OrgId, c.Id)
		}
		case5SkippedMessages.WithLabelValues(c.OrgId).Inc()
		return nil
	}
}

func getName() string {
	name := loremIpsumWords[rand.Intn(len(loremIpsumWords))]

	name = strings.TrimFunc(name, unicode.IsPunct)
	name = strings.Title(name)

	return name
}
