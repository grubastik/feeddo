package main

import (
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/grubastik/feeddo/cmd/feeddo/kafka"
	"github.com/grubastik/feeddo/cmd/feeddo/metrics"
	"github.com/grubastik/feeddo/internal/pkg/heureka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

type AdderCustom struct{ c int32 }

func (ac *AdderCustom) Add(i float64) {
	atomic.AddInt32(&ac.c, int32(i))
}

func TestRunOnce(t *testing.T) {
	URLErr, _ := url.Parse("http://127.0.0.1")
	URL, _ := url.Parse("file://testdata/one_item.xml")
	URLBad, _ := url.Parse("file://testdata/badFeed.xml")
	var a AdderCustom
	mcErr := make(metrics.Container)
	mcErr["a"] = make(map[string]metrics.Adder)
	mcErr["a"]["feed"] = &a
	mc := make(metrics.Container)
	mc[URLBad.String()] = make(map[string]metrics.Adder)
	mc[URLBad.String()]["feed"] = &a
	tests := []struct {
		name     string
		feeds    []*url.URL
		metrics  MetricsGetter
		err      string
		expected heureka.Item
	}{
		{
			"Non existing url",
			[]*url.URL{URLErr},
			nil,
			"Failed to get stream: Unable to download file `http://127.0.0.1` because of Get \"http://127.0.0.1\": dial tcp 127.0.0.1:80: connect: connection refused",
			heureka.Item{},
		},
		{
			"metric Err",
			[]*url.URL{URL},
			mcErr,
			"Failed to get metric: Metric for key 'file://testdata/one_item.xml' is not configured",
			heureka.Item{ID: "34644"},
		},
		{
			"Bad XML",
			[]*url.URL{URLBad},
			mc,
			"Failed to process feed 'file://testdata/badFeed.xml' because of Failed to get item from stream: Failed to unmarshal xml node: XML syntax error on line 21: element <PRODUCTNO> closed by </SHOPITEM>",
			heureka.Item{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chanItem := make(chan kafka.Itemer, 1)
			errs := runOnce(tt.feeds, chanItem, tt.metrics) // this function creates goroutins and wait for them to finish
			close(chanItem)
			if tt.err != "" {
				require.Equal(t, 1, len(errs))
				require.Error(t, errs[0])
				assert.Equal(t, tt.err, errs[0].Error())
			}
			if tt.expected.ID != "" {
				item := <-chanItem
				assert.Equal(t, string(tt.expected.ID), item.GetID())
				assert.Equal(t, 2, len(item.Topics()))
			}
			item := <-chanItem
			assert.Nil(t, item)
		})
	}
}

func TestRunPeriodic(t *testing.T) {
	URLErr, _ := url.Parse("http://127.0.0.1")
	URL, _ := url.Parse("file://testdata/one_item.xml")
	var a AdderCustom
	mc := make(metrics.Container)
	mc[URL.String()] = make(map[string]metrics.Adder)
	mc[URL.String()]["feed"] = &a
	tests := []struct {
		name     string
		feeds    []*url.URL
		metrics  MetricsGetter
		err      string
		expected heureka.Item
	}{
		{
			"Non existing url",
			[]*url.URL{URLErr},
			nil,
			"Failed to get stream: Unable to download file `http://127.0.0.1` because of Get \"http://127.0.0.1\": dial tcp 127.0.0.1:80: connect: connection refused",
			heureka.Item{},
		},
		{
			"happy Path",
			[]*url.URL{URL},
			mc,
			"got termination signal. Exiting",
			heureka.Item{ID: "34644"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chanItem := make(chan kafka.Itemer, 2) // suppose to run twice - for 3 will be blocked forever
			duration := 2 * time.Millisecond       // suppose to run twice - in sync with send signal
			syncSigs := sync.WaitGroup{}
			syncSigs.Add(1)
			chanSig := make(chan os.Signal, 1)
			go func() {
				defer syncSigs.Done()
				<-time.After(3 * time.Millisecond) // suppose to run twice. first round roun immediately
				chanSig <- syscall.SIGINT
			}()
			errs := runPeriodic(tt.feeds, chanItem, duration, chanSig, tt.metrics)
			syncSigs.Wait()
			close(chanItem)
			close(chanSig)
			if tt.err != "" {
				require.Equal(t, 1, len(errs))
				require.Error(t, errs[0])
				assert.Equal(t, tt.err, errs[0].Error())
			}
			if tt.expected.ID != "" {
				//expect to read 2 items
				counter := 0
				for item := range chanItem {
					if item == nil {
						break
					}
					counter++
					assert.Equal(t, string(tt.expected.ID), item.GetID())
					assert.Equal(t, 2, len(item.Topics()))
				}
				assert.Equal(t, 2, counter) //expected to run twice
			}
			item := <-chanItem
			assert.Nil(t, item)
		})
	}
}

// commenting for now - unable to pass mock kafka producer
// type producerSuccess struct{}

// func (pp producerSuccess) Produce(m *kafka.Message, c chan kafka.Event) error {
// 	go func() {
// 		testTopic := "test"
// 		km := &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &testTopic}}
// 		c <- km
// 	}()
// 	return nil
// }

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
