package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grubastik/feeddo/cmd/feeddo/metrics"
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

// MetricsGetter describes interface for metrics container
type MetricsGetter interface {
	GetMetric(string, string) (metrics.Adder, error)
}

type appItem struct {
	shopItem heureka.Item
	feed     string
}

func main() {
	// parse args
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

	//configure app context
	ctx := context.Background()
	ctx = context.WithValue(ctx, "serverAddressMetrics", ":2112")
	ctx, appCancel := context.WithCancel(ctx)

	// create channel for handling termination
	// configure signals
	// App handle signals in the folowing way:
	// when got TERM signal - wait for the full processing of feeds (download/parsing and send them to kafka)
	// stop app after this.
	// it is implemented in this way, because not to get into situation when feed was downloaded but not processed.
	// or was partially processed which leads to inconsistancy in data.
	// if business rules will allow to stop app immediately then handling of this will be even easier.
	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	metricContainer := metrics.NewMetrics(feeds)

	// add waitgroup here for kafka producers
	kafkaWG := sync.WaitGroup{}
	errorWG := sync.WaitGroup{}

	// create channels for kafka produssers
	chanKafkaItem := make(chan appItem) //create a copy of item
	defer close(chanKafkaItem)
	chanError := make(chan error)
	defer close(chanError)
	// we need separated channel from the other to handle specifically error goroutine handler
	// this goroutine should be closed ast one as all of the other goroutines rely on it
	chanErrorClose := make(chan struct{})
	// we do not close next channel now. We need better control on it.
	// this require to be careful with panics
	chanCloseGoroutines := make(chan struct{})
	defer func() {
		// this will close channel will cause goroutine to quite
		if r := recover(); r != nil {
			appCancel()
			close(chanCloseGoroutines)
			kafkaWG.Wait()
			signal.Stop(sigs)
			close(sigs)
			close(chanErrorClose)
			errorWG.Wait()
		}
	}()

	// run kafka producers here
	for i := 0; i < maxProducers; i++ {
		go func() {
			kafkaWG.Add(1)
			defer kafkaWG.Done()
			continueLoop := true
			for continueLoop {
				select {
				// if this channel will be closed - we will go here with default value for item
				case item := <-chanKafkaItem:
					// all items should belong to some feed
					if item.feed != "" {
						m, err := metricContainer.GetMetric(item.feed, "total")
						// in case metric is not available - report error but don't stop the app
						if err != nil {
							chanError <- fmt.Errorf("Failed to get metric: %w", err)
						} else {
							m.Add(1)
						}
						err = putItemToKafka(p, &item.shopItem)
						var errM error
						if err != nil {
							chanError <- err
							m, errM = metricContainer.GetMetric(item.feed, "failed")
						} else {
							m, errM = metricContainer.GetMetric(item.feed, "succeeded")
						}
						// in case metric is not available - report error but don't stop the app
						if errM != nil {
							chanError <- fmt.Errorf("Failed to get metric: %w", err)
						} else {
							m.Add(1)
						}
					}
				case <-chanCloseGoroutines:
					continueLoop = false
				}
			}
		}()
	}
	// log all errors from processing items here
	// will reuse the same close channel as kafka producers
	go func() {
		errorWG.Add(1) // now we have only one error reporter
		defer errorWG.Done()
		continueLoop := true
		for continueLoop {
			select {
			case err := <-chanError:
				//when channel closing we start to always pick this option as default one
				// but this does not mean that error happenned
				if err != nil {
					log.Println(fmt.Errorf("got the following error in app: %w", err))
				}
			case <-chanErrorClose:
				continueLoop = false
			}
		}
	}()

	// run metrics service endpoint
	chanMetricsErr, chanMetricsExit := metrics.RunServer(ctx)
	//monitor metrics servie and forward errors to app channel
	go func() {
		kafkaWG.Add(1)
		defer kafkaWG.Done()
		collectServerErrors := true
		for collectServerErrors {
			select {
			case err := <-chanMetricsErr:
				if err != nil {
					chanError <- err
				}
			case <-chanMetricsExit:
				collectServerErrors = false
			}
		}
	}()

	var err error
	if interval == 0 {
		errs := runOnce(feeds, chanKafkaItem, metricContainer)
		if len(errs) > 0 {
			for _, err = range errs {
				chanError <- fmt.Errorf("One time feeds processing failed: %w", err)
			}
		}
	} else {
		errs := runPeriodic(feeds, chanKafkaItem, interval, sigs, metricContainer)
		if len(errs) > 0 {
			for _, err = range errs {
				chanError <- fmt.Errorf("Periodic feeds processing failed: %w", err)
			}
		}
	}

	//clean up all goroutines
	appCancel()
	close(chanCloseGoroutines)
	kafkaWG.Wait()
	//close error channel when all other gorutines exited
	close(chanErrorClose)
	errorWG.Wait()

	//closing signals channel
	// we do not process them anymore
	signal.Stop(sigs)
	close(sigs)

	return err
}

func runPeriodic(feeds []*url.URL, chanKafkaItem chan<- appItem, interval time.Duration, chanCloseApp <-chan os.Signal, metrics MetricsGetter) []error {
	t := time.NewTicker(interval)
	defer t.Stop()
	// ticker do not run processing strait ahead
	errs := runOnce(feeds, chanKafkaItem, metrics)
	if len(errs) != 0 {
		return errs
	}
	processing := false // handle situation when someone wanted to process feeds too often
	runApp := true      // use to break app execution
	done := make(chan struct{})
	defer close(done)
	// handle error situation - breaks execution of tool
	errChan := make(chan error)
	defer close(errChan)
	var err error
	for {
		select {
		case <-chanCloseApp:
			runApp = false
		case err = <-errChan:
			if err != nil {
				errs = append(errs, err)
			}
		// when processing of all feeds done - this channel will be triggered
		case <-done:
			processing = false
		case <-t.C:
			//do not run next round if we already processing feeds or error happenned
			if !processing && len(errs) == 0 {
				processing = true
				go func() {
					errs := runOnce(feeds, chanKafkaItem, metrics)
					for _, err := range errs {
						errChan <- err
					}
					done <- struct{}{}
				}()
			}
		}
		// close app in case of error
		if !processing && len(errs) != 0 {
			break
		}
		// cloase app if got ctrl-break
		if !processing && !runApp {
			errs = []error{errors.New("Got termination signal. Exiting")}
			break
		}
	}
	return errs
}

func runOnce(feeds []*url.URL, chanKafkaItem chan<- appItem, mC MetricsGetter) []error {
	// consider errChan to be notication of finishing processing
	// if succeded - return nil
	// on error return struct with error
	errChan := make(chan error)
	defer close(errChan)
	for _, u := range feeds {
		go func(u *url.URL) {
			m, err := mC.GetMetric(u.String(), "feed")
			// in case metric is not available - report error but don't stop the app
			if err != nil {
				errChan <- fmt.Errorf("Failed to get metric: %w", err)
			} else {
				m.Add(1)
				defer m.Add(-1)
			}

			err = processFeed(u, chanKafkaItem)
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

func processFeed(u *url.URL, chanKafkaItem chan<- appItem) error {
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
			chanKafkaItem <- appItem{shopItem: *item, feed: u.String()}
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
