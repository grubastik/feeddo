package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		err           string
		feedExpected  []string
		kafkaExpected string
	}{
		{
			name:          "Empty feed and kafka",
			args:          []string{"test"},
			err:           "Unable to parse flags: the required flags `-f, --feedUrl' and `-k, --kafkaUrl' were not specified",
			feedExpected:  nil,
			kafkaExpected: "",
		},
		{
			name:          "Empty feed",
			args:          []string{"test", "-k", "test.org"},
			err:           "Unable to parse flags: the required flag `-f, --feedUrl' was not specified",
			feedExpected:  nil,
			kafkaExpected: "",
		},
		{
			name:          "Empty kafka",
			args:          []string{"test", "-f", "http://test.org"},
			err:           "Unable to parse flags: the required flag `-k, --kafkaUrl' was not specified",
			feedExpected:  nil,
			kafkaExpected: "",
		},
		{
			name:          "single feed and kafka",
			args:          []string{"test", "-f", "http://test.org", "-k", "test.org"},
			err:           "",
			feedExpected:  []string{"http://test.org"},
			kafkaExpected: "test.org",
		},
		{
			name:          "multiple feed and single kafka",
			args:          []string{"test", "-f", "http://test.org", "-f", "http://test.other.org", "-k", "test.org"},
			err:           "",
			feedExpected:  []string{"http://test.org", "http://test.other.org"},
			kafkaExpected: "test.org",
		},
		{
			name:          "multiple feed and multiple kafka",
			args:          []string{"test", "-f", "http://test.org", "-f", "http://test.other.org", "-k", "test.org", "-k", "test.other.org"},
			err:           "",
			feedExpected:  []string{"http://test.org", "http://test.other.org"},
			kafkaExpected: "test.other.org",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = tt.args
			feeds, kafka, duration, err := parseArgs()
			if tt.err != "" {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				require.NoError(t, err)
				for i, f := range feeds {
					assert.Equal(t, tt.feedExpected[i], f.String())
				}
				assert.Equal(t, tt.kafkaExpected, kafka)
				assert.Equal(t, time.Duration(0), duration)
			}
		})
	}
}

type producerSuccess struct{}

func (pp producerSuccess) Produce(m *kafka.Message, c chan kafka.Event) error {
	go func() {
		testTopic := "test"
		km := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &testTopic}}
		c <- km
	}()
	return nil
}

// commenting for now - unable to pass mock kafka producer
// func BenchmarkRunOnce(b *testing.B) {
// 	feeds := make([]*url.URL, 2, 2)
// 	for i, str := range []string{"file://testdata/107090_items.xml", "file://testdata/400000_items.xml"} {
// 		u, err := url.Parse(str)
// 		require.NoError(b, err)
// 		feeds[i] = u
// 	}
// 	p := producerSuccess{}
// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		err := appRun(feeds, p, 0)
// 		require.NoError(b, err)
// 	}
// }
