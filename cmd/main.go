package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/grubastik/feeddo/cmd/heureka"
	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	feeds, kafkaURL, interval, err := parseArgs()
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to parse flags: %w", err))
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaURL})
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to init connection to Kafka: %w", err))
	}
	defer p.Close()

	if interval == 0 {
		err := runProcess(feeds, p)
		if err != nil {
			log.Fatal(fmt.Errorf("Feeds processing failed: %w", err))
		}
	} else {
		t := time.NewTicker(interval)
		defer t.Stop()
		// ticker do not run processing strait ahead
		err := runProcess(feeds, p)
		if err != nil {
			log.Fatal(fmt.Errorf("Feeds processing failed: %w", err))
		}
		processing := false
		// handle situation when someone wanted to process feeds too often
		done := make(chan struct{})
		defer close(done)
		// handle error situation - breaks execution of tool
		errChan := make(chan error)
		defer close(errChan)
		for {
			select {
			case err = <-errChan:
			case <-done:
				processing = false
			case <-t.C:
				if !processing {
					processing = true
					go func() {
						err := runProcess(feeds, p)
						if err != nil {
							errChan <- err
						}
						done <- struct{}{}
					}()
				}
			}
			if err != nil {
				break
			}
		}
		if err != nil {
			log.Fatal(fmt.Errorf("Feeds periodic processing failed: %w", err))
		}
	}
}

func runProcess(feeds []*url.URL, p Producer) error {
	for _, url := range feeds {
		err := processFeed(url, p)
		if err != nil {
			return fmt.Errorf("Failed to process feed '%s' because of %w", url.String(), err)
		}
	}
	return nil
}

func parseArgs() ([]*url.URL, string, time.Duration, error) {
	var opts struct {
		// list of feeds' urls
		URLs           []string `short:"f" long:"feedUrl" description:"Provide url to feeds. Can beused multiple times" required:"true"`
		KafkaURL       string   `short:"k" long:"kafkaUrl" description:"Url to connect to kafka" required:"true"`
		RepeatInterval string   `short:"i" long:"interval" description:"Interval after which we will make another attempt to download feeds. If '0' is provided then we run process only once. Supported values are supported values by time.Duration in golang"`
	}
	parser := flags.NewParser(&opts, flags.PassDoubleDash|flags.IgnoreUnknown)
	_, err := parser.Parse()
	if err != nil {
		return nil, "", 0, fmt.Errorf("Unable to parse flags: %w", err)
	}
	if len(opts.URLs) == 0 {
		return nil, "", 0, fmt.Errorf("List of feed URLs was not provided")
	}
	feeds := []*url.URL{}
	for _, u := range opts.URLs {
		url, err := url.Parse(u)
		if err != nil {
			return nil, "", 0, fmt.Errorf("Unable to parse feed url '%s' because of %w", u, err)
		}
		feeds = append(feeds, url)
	}
	if opts.KafkaURL == "" {
		return nil, "", 0, fmt.Errorf("Kafka url was not provided")
	}

	duration := time.Duration(0)
	if opts.RepeatInterval != "" {
		duration, err = time.ParseDuration(opts.RepeatInterval)
		if err != nil {
			return nil, "", 0, fmt.Errorf("Failed to parse duration because of %w", err)
		}
	}

	return feeds, opts.KafkaURL, duration, nil
}

func processFeed(u *url.URL, p Producer) error {
	//create stream from response to save some memory and speedup processing
	readCloser, err := createStream(u)
	if err != nil {
		return fmt.Errorf("Failed to get stream: %w", err)
	}
	defer readCloser.Close()
	// try to unmarshal stream.
	// If this stream is not represent expected schema - result will be empty.
	d := xml.NewDecoder(readCloser)
	for {
		item, err := getItemFromStream(d)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			} else {
				return fmt.Errorf("Failed to unmarshal xml: %w", err)
			}
		}
		if item != nil {
			message, err := json.Marshal(item)
			if err != nil {
				return fmt.Errorf("Failed to marshal json: %w", err)
			}
			// Produce messages to topic (asynchronously)
			topic := "shop_items"
			err = sendItemToKafka(p, topic, message)
			if err != nil {
				return fmt.Errorf("Failed to send message to topic %s because of: %w", topic, err)
			}
			if !item.HeurekaCPC.Equal(decimal.Zero) {
				topic := "shop_items_bidding"
				err := sendItemToKafka(p, topic, message)
				if err != nil {
					return fmt.Errorf("Failed to send message to topic %s because of: %w", topic, err)
				}
			}
		}
	}
	return nil
}

func createStream(u *url.URL) (io.ReadCloser, error) {
	var readCloser io.ReadCloser
	var err error
	if u.Scheme == "file" {
		readCloser, err = os.Open(u.Hostname() + u.EscapedPath())
		if err != nil {
			return nil, fmt.Errorf("Unable to read file `%v` because of %w", u, err)
		}
	} else {
		resp, err := http.Get(u.String())
		if err == nil && resp.Body != nil {
			readCloser = resp.Body
		}
		if err != nil {
			return nil, fmt.Errorf("Unable to download file `%v` because of %w", u, err)
		}
	}
	return readCloser, nil
}

// Decoder implements xml decode interface
type Decoder interface {
	Token() (xml.Token, error)
	DecodeElement(v interface{}, start *xml.StartElement) error
}

// getItemFromStream retrieves next item from xml
// item can be nil if start tag of next element in feed will be not recognized
// in this case error not provided and also will be nil
func getItemFromStream(d Decoder) (*heureka.Item, error) {
	token, err := d.Token()
	if err != nil {
		return nil, fmt.Errorf("Failed to read node element: %w", err)
	}
	switch startElem := token.(type) {
	case xml.StartElement:
		if startElem.Name.Local == "SHOPITEM" {
			item := &heureka.Item{}
			err = d.DecodeElement(item, &startElem)
			if err != nil {
				return nil, fmt.Errorf("Failed to unmarshal xml node: %w", err)
			}
			return item, nil
		}
	default:
	}
	return nil, nil
}

// Producer for kafka topics
type Producer interface {
	Produce(*kafka.Message, chan kafka.Event) error
}

func sendItemToKafka(p Producer, topic string, m []byte) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(m),
	}
	err := p.Produce(km, deliveryChan)
	if err != nil {
		return fmt.Errorf("Send message to kafka failed because of %w", err)
	}

	// add timeout here to not block up forever
	ke := <-deliveryChan
	km, ok := ke.(*kafka.Message)
	if !ok {
		return fmt.Errorf("Failed to cast message from channel to kafka message: %v", ke)
	}
	if km.TopicPartition.Error != nil {
		return fmt.Errorf("Delivery to kafka failed: %w", km.TopicPartition.Error)
	}

	log.Printf("Delivered message to topic %s [%d] at offset %v\n",
		*km.TopicPartition.Topic, km.TopicPartition.Partition, km.TopicPartition.Offset)
	return nil
}
