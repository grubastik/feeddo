package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/grubastik/feeddo/internal/pkg/heureka"
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

type producerError struct{}

func (pp producerError) Produce(m *kafka.Message, c chan kafka.Event) error {
	return errors.New("test error")
}

type producerChannelError struct{}

func (pp producerChannelError) Produce(m *kafka.Message, c chan kafka.Event) error {
	go func() {
		km := &kafka.Message{TopicPartition: kafka.TopicPartition{Error: errors.New("Test channel error")}}
		c <- km
	}()
	return nil
}

func TestSendItemToKafka(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		message  []byte
		producer Producer
		err      string
	}{
		{
			name:     "Producer failed",
			topic:    "test",
			message:  []byte("test"),
			producer: producerError{},
			err:      "Send message to kafka failed because of test error",
		},
		{
			name:     "Producer failed to deliver message to kafka",
			topic:    "test",
			message:  []byte("test"),
			producer: producerChannelError{},
			err:      "Delivery to kafka failed: Test channel error",
		},
		{
			name:     "happy path",
			topic:    "test",
			message:  []byte("test"),
			producer: producerSuccess{},
			err:      "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sendMessageToKafka(tt.producer, tt.topic, tt.message)
			if tt.err != "" {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type decoderTokenErr struct{}

func (t decoderTokenErr) Token() (xml.Token, error) {
	return nil, errors.New("Test token")
}

func (t decoderTokenErr) DecodeElement(v interface{}, start *xml.StartElement) error {
	return nil
}

type decoderDecodeErr struct{}

func (t decoderDecodeErr) Token() (xml.Token, error) {
	return xml.StartElement{Name: xml.Name{Local: "SHOPITEM"}}, nil
}

func (t decoderDecodeErr) DecodeElement(v interface{}, start *xml.StartElement) error {
	return errors.New("Test decode error")
}

type decoderDecodeWrongElement struct{}

func (t decoderDecodeWrongElement) Token() (xml.Token, error) {
	return xml.EndElement{Name: xml.Name{Local: "SHOPITEM"}}, nil
}

func (t decoderDecodeWrongElement) DecodeElement(v interface{}, start *xml.StartElement) error {
	return errors.New("Test decode error")
}

type decoderDecodeWrongTagName struct{}

func (t decoderDecodeWrongTagName) Token() (xml.Token, error) {
	return xml.EndElement{Name: xml.Name{Local: "SOMEITEM"}}, nil
}

func (t decoderDecodeWrongTagName) DecodeElement(v interface{}, start *xml.StartElement) error {
	return errors.New("Test decode error")
}

type decoderHappyPath struct{}

func (t decoderHappyPath) Token() (xml.Token, error) {
	return xml.StartElement{Name: xml.Name{Local: "SHOPITEM"}}, nil
}

func (t decoderHappyPath) DecodeElement(v interface{}, start *xml.StartElement) error {
	ir, ok := v.(*heureka.Item)
	if !ok {
		return errors.New("Can not cast interface to heureka item")
	}
	*ir = heureka.Item{Product: "Test", ProductName: "TestName"}
	return nil
}

func TestGetItemFromStream(t *testing.T) {
	tests := []struct {
		name    string
		decoder Decoder
		err     string
		item    *heureka.Item
	}{
		{
			name:    "token error",
			decoder: decoderTokenErr{},
			err:     "Failed to read node element: Test token",
			item:    nil,
		},
		{
			name:    "decoding error",
			decoder: decoderDecodeErr{},
			err:     "Failed to unmarshal xml node: Test decode error",
			item:    nil,
		},
		{
			name:    "decoding without results",
			decoder: decoderDecodeWrongElement{},
			err:     "",
			item:    nil,
		},
		{
			name:    "decoding with wrong tag name",
			decoder: decoderDecodeWrongTagName{},
			err:     "",
			item:    nil,
		},
		{
			name:    "happy path",
			decoder: decoderHappyPath{},
			err:     "",
			item:    &heureka.Item{Product: "Test", ProductName: "TestName"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i, err := getItemFromStream(tt.decoder)
			if tt.err != "" {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.item, i)
			}
		})
	}
}

func TestCreateStream(t *testing.T) {
	tests := []struct {
		name      string
		URL       string
		err       string
		isFile    bool
		runServer bool
	}{
		{
			name:      "non existing file",
			URL:       "file:///test.xml",
			err:       "Unable to read file `file:///test.xml` because of open /test.xml: no such file or directory",
			isFile:    true,
			runServer: false,
		},
		{
			name:      "file success",
			URL:       "file://testdata/one_item.xml",
			err:       "",
			isFile:    true,
			runServer: false,
		},
		{
			name:      "wrong url",
			URL:       "http://localhost:8945",
			err:       "connect: connection refused",
			isFile:    false,
			runServer: false,
		},
		{
			name:      "success download",
			URL:       "",
			err:       "",
			isFile:    false,
			runServer: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.runServer {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					fmt.Fprintln(w, "Hello, there")
				}))
				defer ts.Close()

				tt.URL = ts.URL
			}

			u, err := url.Parse(tt.URL)
			require.NoError(t, err)
			stream, err := createStream(u)
			if stream != nil {
				defer stream.Close()
			}
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, stream)
				if tt.isFile {
					assert.IsType(t, &os.File{}, stream)
				} else {
					_, ok := stream.(io.ReadCloser)
					assert.True(t, ok)
				}
			}
		})
	}
}

func BenchmarkRunOnce(b *testing.B) {
	feeds := make([]*url.URL, 2, 2)
	for i, str := range []string{"file://testdata/107090_items.xml", "file://testdata/400000_items.xml"} {
		u, err := url.Parse(str)
		require.NoError(b, err)
		feeds[i] = u
	}
	p := producerSuccess{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := appRun(feeds, p, 0)
		require.NoError(b, err)
	}
}
