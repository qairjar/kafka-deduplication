package kafkadeduplication

import (
	"encoding/json"
	"github.com/artyomturkin/go-from-uri/kafka"
	"github.com/artyomturkin/saramahelper"
	"reflect"
	"time"
)

type SaramaConfig struct {
	Brokers   string
	TopicName string
	Size      int
	Msgs      []map[string]interface{}
}

func (s *SaramaConfig) CacheBuilder() error {
	sc, err := kafka.NewSaramaClient(s.Brokers)
	if err != nil {
		return err
	}
	msgCh, errs := saramahelper.Fetch(sc, s.TopicName, s.Size)

	for err := range errs {
		if err != nil {
			return err
		}
	}

	var msgs []map[string]interface{}
	for m := range msgCh {
		var msg map[string]interface{}
		err := json.Unmarshal(m.Value, &msg)
		if err != nil {
			return err
		}
		msgs = append(msgs, msg)
	}
	s.Msgs = msgs
	return nil
}

func (s *SaramaConfig) GetLastTimestamp() (error, time.Time) {

	var t time.Time
	for _, msg := range s.Msgs {
		if value, ok := msg["current_ts"].(time.Time); ok {
			if value.Unix() > t.Unix() {
				t = value
			}
		}
	}
	return nil, t
}

func (s *SaramaConfig) EqualMsg(msg map[string]interface{}) bool {
	for _, m := range s.Msgs {
		eq := reflect.DeepEqual(msg, m)
		if eq {
			return eq
		}
	}
	return false
}
