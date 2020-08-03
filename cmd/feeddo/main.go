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
	"strings"
	"sync"
	"time"

	"github.com/grubastik/feeddo/internal/pkg/heureka"
	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	// ideally we will need to adjustthis number based on the number of cores
	maxProducers = 10
)

func main() {
	feeds, kafkaURL, interval, err := parseArgs()
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to parse flags: %w", err))
	}
	// all options could be found here https://docs.confluent.io/5.5.0/clients/librdkafka/md_CONFIGURATION.html
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":              kafkaURL,
		"socket.timeout.ms":              5000,
		"request.timeout.ms":             5000,
		"message.timeout.ms":             5000,
		"delivery.timeout.ms":            5000,
		"metadata.request.timeout.ms":    5000,
		"api.version.request.timeout.ms": 5000,
		"transaction.timeout.ms":         5000,
		"socket.keepalive.enable":        true,
	})
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to init connection to Kafka: %w", err))
	}
	defer p.Close()

	err = appRun(feeds, p, interval)
	if err != nil {
		os.Exit(1) //non zero exit code identifies error
	}
}

func appRun(feeds []*url.URL, p Producer, interval time.Duration) error {
	// add waitgroup here for kafka producers
	kafkaWG := sync.WaitGroup{}
	kafkaWG.Add(maxProducers + 1) // +1 indicates error producer

	// create channels for kafka produssers
	chanKafkaItem := make(chan heureka.Item) //create a copy of item
	defer close(chanKafkaItem)
	chanKafkaError := make(chan error)
	defer close(chanKafkaError)
	// we do not close next channel now. We need better control on it.
	// this require to be careful with panics
	chanKafkaClose := make(chan struct{})
	defer func() {
		// this will close channel will cause goroutine to quite
		if r := recover(); r != nil {
			close(chanKafkaClose)
			kafkaWG.Wait()
		}
	}()

	// run kafka producers here
	for i := 0; i < maxProducers; i++ {
		go func() {
			defer kafkaWG.Done()
			continueLoop := true
			for continueLoop {
				select {
				case item := <-chanKafkaItem:
					err := putItemToKafka(p, &item)
					if err != nil {
						chanKafkaError <- err
					}
				case <-chanKafkaClose:
					continueLoop = false
				}
			}
		}()
	}
	// log all errors from processing items here
	// will reuse the same close channel as kafka producers
	go func() {
		defer kafkaWG.Done()
		continueLoop := true
		for continueLoop {
			select {
			case err := <-chanKafkaError:
				log.Println(fmt.Errorf("Processing of item in kafka producer failed: %w", err))
			case <-chanKafkaClose:
				continueLoop = false
			}
		}
	}()

	var err error
	if interval == 0 {
		errs := runOnce(feeds, chanKafkaItem)
		if len(errs) > 0 {
			for _, err = range errs {
				log.Println(fmt.Errorf("Onetie feeds processing failed: %w", err))
			}
		}
	} else {
		errs := runPeriodic(feeds, chanKafkaItem, interval)
		if len(errs) > 0 {
			for _, err = range errs {
				log.Println(fmt.Errorf("Periodic feeds processing failed: %w", err))
			}
		}
	}

	//clean up all goroutines
	close(chanKafkaClose)
	kafkaWG.Wait()

	return err
}

func runPeriodic(feeds []*url.URL, chanKafkaItem chan<- heureka.Item, interval time.Duration) []error {
	t := time.NewTicker(interval)
	defer t.Stop()
	// ticker do not run processing strait ahead
	errs := runOnce(feeds, chanKafkaItem)
	if len(errs) != 0 {
		return errs
	}
	processing := false // handle situation when someone wanted to process feeds too often
	done := make(chan struct{})
	defer close(done)
	// handle error situation - breaks execution of tool
	errChan := make(chan error)
	defer close(errChan)
	for {
		var err error
		select {
		case err = <-errChan:
			if err != nil {
				errs = append(errs, err)
			}
		// when processing of all feeds done - this channel will be triggered
		case <-done:
			processing = false
		case <-t.C:
			//do not run next round if we already processing feeds or error happenned
			if !processing && len(errs) != 0 {
				processing = true
				go func() {
					errs := runOnce(feeds, chanKafkaItem)
					for _, err := range errs {
						errChan <- err
					}
					done <- struct{}{}
				}()
			}
		}
		if !processing && len(errs) != 0 {
			break
		}
	}
	return errs
}

func runOnce(feeds []*url.URL, chanKafkaItem chan<- heureka.Item) []error {
	// consider errChan to be notication of finishing processing
	// if succeded - return nil
	// on error return struct with error
	errChan := make(chan error)
	defer close(errChan)
	for _, u := range feeds {
		go func(u *url.URL) {
			err := processFeed(u, chanKafkaItem)
			if err == nil {
				errChan <- nil
			} else {
				errChan <- fmt.Errorf("Failed to process feed '%s' because of %w", u.String(), err)
			}
		}(u)
	}
	//block execution until all goroutines will be finished
	errs := make([]error, 0, 0)
	for i := 0; i < len(feeds); i++ {
		select {
		case err := <-errChan:
			if err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errs
}

func parseArgs() ([]*url.URL, string, time.Duration, error) {
	var opts struct {
		// list of feeds' urls
		URLs           []string `short:"f" long:"feedUrl" description:"Provide url to feeds. Can beused multiple times" required:"true" env:"FEED_URLS" env-delim:","`
		KafkaURL       string   `short:"k" long:"kafkaUrl" description:"Url to connect to kafka" required:"true" env:"KAFKA_URL"`
		RepeatInterval string   `short:"i" long:"interval" description:"Interval after which we will make another attempt to download feeds. If '0' is provided then we run process only once. Supported values are supported values by time.Duration in golang" env:"REPEAT_INTERVAL"`
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
		url, err := url.Parse(strings.TrimSpace(u))
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

func processFeed(u *url.URL, chanKafkaItem chan<- heureka.Item) error {
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
			chanKafkaItem <- *item
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

func putItemToKafka(p Producer, item *heureka.Item) error {
	message, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("Failed to marshal json: %w", err)
	}
	// Produce messages to topic (asynchronously)
	topic := "shop_items"
	err = sendMessageToKafka(p, topic, message)
	if err != nil {
		return fmt.Errorf("Failed to send message to topic %s because of: %w", topic, err)
	}
	if !item.HeurekaCPC.Equal(decimal.Zero) {
		topic := "shop_items_bidding"
		err := sendMessageToKafka(p, topic, message)
		if err != nil {
			return fmt.Errorf("Failed to send message to topic %s because of: %w", topic, err)
		}
	}
	return nil
}

func sendMessageToKafka(p Producer, topic string, m []byte) error {
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

	return nil
}
