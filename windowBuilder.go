package kafkadeduplication

import (
	"github.com/Shopify/sarama"
	"github.com/artyomturkin/go-from-uri/kafka"
	"github.com/artyomturkin/saramahelper"
	"time"
)

type CacheBuilder struct {
	From       time.Time
	To         time.Time
	WindowSize time.Time
	Lag        time.Duration
	InitFrom   time.Time
	Delay      time.Duration
	Messages   []*sarama.ConsumerMessage
}

type SaramaConfig struct {
	Brokers   string
	TopicName string
	Size      int
}

func (c *CacheBuilder) ArgsBuilder() {
	srmCnf := &SaramaConfig{
		Brokers:   "kafka://localhost:9092",
		TopicName: "test",
		Size:      5,
	}
	sc, err := kafka.NewSaramaClient(srmCnf.Brokers)
	if err != nil {
		panic(err)
	}

	msgCh, errch := saramahelper.Fetch(sc, srmCnf.TopicName, srmCnf.Size)

	var errs []error
	for err := range errch {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return
	}

	var fromTime time.Time
	for m := range msgCh {
		c.Messages = append(c.Messages, m)
		if fromTime.Unix() < m.Timestamp.Unix() {
			fromTime = m.Timestamp
		}
	}

	if len(c.Messages) > 0 {
		c.From = fromTime
		c.To = time.Now()
	} else if c.From.IsZero() && c.To.IsZero() {
		if !c.InitFrom.IsZero() {
			c.From = c.InitFrom
		}
		c.To = time.Now()
	}

}
