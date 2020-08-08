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

	"github.com/grubastik/feeddo/cmd/feeddo/kafka"
	"github.com/grubastik/feeddo/cmd/feeddo/metrics"
	"github.com/grubastik/feeddo/internal/pkg/heureka"
	"github.com/jessevdk/go-flags"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
)

const (
	// ideally we will need to adjustthis number based on the number of cores
	maxProducers = 10
	// local address where metrics server will listen for connections
	metricsAddress = ":2112"
)

// MetricsGetter describes interface for metrics container
type MetricsGetter interface {
	GetMetric(string, string) (metrics.Adder, error)
}

type appItem struct {
	shopItem heureka.Item
	feed     string
	topics   []string
}

func (ai appItem) GetContext() string       { return ai.feed }
func (ai appItem) GetID() string            { return string(ai.shopItem.ID) }
func (ai appItem) Marshal() ([]byte, error) { return json.Marshal(ai.shopItem) }
func (ai appItem) Topics() []string         { return ai.topics }

func main() {
	// parse args
	feeds, kafkaURL, interval, err := parseArgs()
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to parse flags: %w", err))
	}

	err = appRun(feeds, kafkaURL, interval)

	if err != nil {
		os.Exit(1) //non zero exit code identifies error
	}
}

func appRun(feeds []*url.URL, kafkaURL string, interval time.Duration) error {
	//configure app context
	ctx := context.Background()
	ctx = context.WithValue(ctx, metrics.MetricsAddressCtxKey, metricsAddress)
	ctx, appCancelFunc := context.WithCancel(ctx)
	// build kafka context
	ctxKafka := context.WithValue(ctx, kafka.KafkaAddressCtxKey, kafkaURL)
	ctxKafka = context.WithValue(ctxKafka, kafka.MaxProducersCtxKey, maxProducers)
	ctxKafka, kafkaCancelFunc := context.WithCancel(ctxKafka)
	//init kafka
	p, err := kafka.NewKafkaProducer(ctxKafka)
	if err != nil {
		kafkaCancelFunc()
		appCancelFunc()
		return fmt.Errorf("Failed to start kafka producer: %w", err)
	}

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

	// add waitgroup for error consumer
	errorWG := sync.WaitGroup{}
	appWG := sync.WaitGroup{}

	// create channel for kafka produssers
	chanKafkaItem := make(chan kafka.Itemer) //create a copy of item
	defer close(chanKafkaItem)
	// create channel for error handling
	chanError := make(chan error)
	defer close(chanError)
	// we need separated channel from the other to handle specifically error goroutine handler
	// this goroutine should be closed ast one as all of the other goroutines rely on it
	chanErrorClose := make(chan struct{})
	defer func() {
		// this will close channel will cause goroutine to quite
		if r := recover(); r != nil {
			kafkaCancelFunc()
			appCancelFunc()
			signal.Stop(sigs)
			close(sigs)
			close(chanErrorClose)
			errorWG.Wait()
		}
	}()

	// run kafka producers here
	chanKafkaRes, chanKafkaExited := p.CreateProducersPool(chanKafkaItem)

	// log all errors from processing items here
	// will reuse the same close channel as kafka producers
	errorWG.Add(1) // now we have only one error reporter
	go func() {
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
	appWG.Add(2)
	go func() {
		defer appWG.Done()
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

	go func() {
		defer appWG.Done()
		collectKafkaErrors := true
		for collectKafkaErrors {
			select {
			case res := <-chanKafkaRes:
				if res.ItemContext != "" {
					var errM error
					errM = incrementMetric(metricContainer, res.ItemContext, metrics.MetricTypeTotal)
					// in case metric is not available - report error but don't stop the app
					if errM != nil {
						chanError <- err
					}
					if res.Err != nil {
						chanError <- res.Err
						errM = incrementMetric(metricContainer, res.ItemContext, metrics.MetricTypeFailed)
					} else {
						errM = incrementMetric(metricContainer, res.ItemContext, metrics.MetricTypeSucceeded)
					}
					// in case metric is not available - report error but don't stop the app
					if errM != nil {
						chanError <- err
					}
				}
			case <-chanKafkaExited:
				collectKafkaErrors = false
			}
		}
	}()

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
	kafkaCancelFunc()
	appCancelFunc()
	//close error channel when all other gorutines exited
	close(chanErrorClose)
	errorWG.Wait()

	//closing signals channel
	// we do not process them anymore
	signal.Stop(sigs)
	close(sigs)

	return nil
}

func incrementMetric(mc metrics.Container, context, metricType string) error {
	m, err := mc.GetMetric(context, metricType)
	if err != nil {
		return fmt.Errorf("Failed to get metric: %w", err)
	}
	m.Add(1)
	return nil
}

func runPeriodic(feeds []*url.URL, chanKafkaItem chan<- kafka.Itemer, interval time.Duration, chanCloseApp <-chan os.Signal, metrics MetricsGetter) []error {
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

func runOnce(feeds []*url.URL, chanKafkaItem chan<- kafka.Itemer, mC MetricsGetter) []error {
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

func processFeed(u *url.URL, chanKafkaItem chan<- kafka.Itemer) error {
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
			topics := []string{kafka.TopicShopItems}
			if !item.HeurekaCPC.Equal(decimal.Zero) {
				topics = append(topics, kafka.TopicShopItemsBidding)
			}
			chanKafkaItem <- appItem{shopItem: *item, feed: u.String(), topics: topics}
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
