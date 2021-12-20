package kafkadeduplication

import (
	"encoding/json"
	"fmt"
	"github.com/artyomturkin/go-from-uri/kafka"
	"github.com/artyomturkin/saramahelper"
	"time"
)

type SaramaConfig struct {
	Brokers   string
	TopicName string
	Size      int
}


type Cache struct {
	Msgs []map[string]interface{}
	Count int
}

func (m *Cache) CacheBuilder(s *SaramaConfig) error {
	sc, err := kafka.NewSaramaClient(s.Brokers)
	if err != nil {
		return err
	}
	msgCh, errs := saramahelper.Fetch(sc, s.TopicName, s.Size)

	var msgs []map[string]interface{}
	for m := range msgCh {
		var msg map[string]interface{}
		err := json.Unmarshal(m.Value, &msg)
		if err != nil {
			return err
		}
		msgs = append(msgs, msg)
	}
	m.Count = len(msgs)
	m.Msgs = msgs

	for err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Cache) GetLastTimestamp() (error, time.Time) {

	var t time.Time
	for _, msg := range c.Msgs {
		if value, ok := msg["current_ts"].(time.Time); ok {
			if value.Unix() > t.Unix() {
				t = value
			}
		}
	}
	return nil, t
}

func (c *Cache) EqualMsg(msg map[string]interface{}) bool {
	for _, m := range c.Msgs {
		result := true
		for key, item := range m {
			if value, ok := msg[key]; ok {
				if t, ok := value.(time.Time); ok {
					ts := t.Format(time.RFC3339Nano)
					if fmt.Sprint(item) != ts {
						result = false
					}
				}else{
					if fmt.Sprint(item) != fmt.Sprint(value) {
						result = false
					}
				}
			}
		}
		if result {
			return true
		}
	}
	return false
}