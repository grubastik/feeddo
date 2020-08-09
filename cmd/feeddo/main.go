package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grubastik/feeddo/cmd/feeddo/kafka"
	"github.com/grubastik/feeddo/cmd/feeddo/metrics"
	"github.com/grubastik/feeddo/cmd/feeddo/parser"
	"github.com/grubastik/feeddo/cmd/feeddo/provider"
	"github.com/grubastik/feeddo/internal/pkg/heureka"
	"github.com/jessevdk/go-flags"
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
	defer func() {
		//closing signals channel
		// we do not process them anymore
		signal.Stop(sigs)
		close(sigs)
	}()

	// prepare error handling
	// create channel for error handling
	// this channel should be closed last one to prevent panic
	// that is why error processing is the first goroutine to start
	chanError := make(chan error)
	defer close(chanError)
	// create context for error
	ctxError, errorCancelFunc := context.WithCancel(ctx)
	defer errorCancelFunc()
	// create waitgroup for error consumer
	errorWG := sync.WaitGroup{}
	errorWG.Add(1) // now we have only one error reporter
	go func() {
		defer errorWG.Done()
		processErrors(ctxError, chanError)
	}()

	//run metrics service
	// metrics context
	ctxMetrics := context.WithValue(ctx, metrics.MetricsAddressCtxKey, metricsAddress)
	ctxMetrics, metrixCancelFunc := context.WithCancel(ctxMetrics)
	defer metrixCancelFunc()
	metricContainer := metrics.NewMetrics(feeds)
	// run metrics service endpoint
	chanMetricsErr, chanMetricsExit := metrics.RunServer(ctxMetrics)

	// run kafka producers
	// build kafka context
	ctxKafka := context.WithValue(ctx, kafka.KafkaAddressCtxKey, kafkaURL)
	ctxKafka = context.WithValue(ctxKafka, kafka.MaxProducersCtxKey, maxProducers)
	ctxKafka, kafkaCancelFunc := context.WithCancel(ctxKafka)
	defer kafkaCancelFunc()
	//init kafka
	p, err := kafka.NewKafkaProducer(ctxKafka)
	if err != nil {
		return fmt.Errorf("Failed to start kafka producer: %w", err)
	}
	// create channel for kafka produssers
	chanKafkaItem := make(chan kafka.Itemer) //create a copy of item
	defer close(chanKafkaItem)
	// run kafka producers
	chanKafkaRes, chanKafkaExited := p.CreateProducersPool(chanKafkaItem)

	//create waitgroup for app service goroutines
	appWG := sync.WaitGroup{}
	appWG.Add(1)
	//monitor metrics errors and forward them to error channel
	go func() {
		defer appWG.Done()
		redirectMetricsErrorsToErrors(chanMetricsErr, chanError, chanMetricsExit)
	}()

	//monitor populating items to kafka: redirect errors to error channel and also collect metrics
	appWG.Add(1)
	go func() {
		defer appWG.Done()
		processKafkaRes(chanKafkaRes, chanError, chanKafkaExited, metricContainer)
	}()

	//this is the main execution part which triggers all the notifications in channels
	if interval == 0 {
		errs := runOnce(feeds, chanKafkaItem, metricContainer)
		if len(errs) > 0 {
			for _, err = range errs {
				// not always: metrics can generate errors but feeds still will be processed
				chanError <- fmt.Errorf("One time feeds processing failed: %w", err)
			}
		}
	} else {
		errs := runPeriodic(feeds, chanKafkaItem, interval, sigs, metricContainer)
		if len(errs) > 0 {
			for _, err = range errs {
				// not always: metrics can generate errors but feeds still will be processed
				chanError <- fmt.Errorf("Periodic feeds processing failed: %w", err)
			}
		}
	}

	//clean up all goroutines
	// first stop kafka producers
	kafkaCancelFunc()
	// cancel metrix processing
	metrixCancelFunc()
	// wait for errors to stop
	errorWG.Wait()

	return nil
}

func processKafkaRes(chanKafkaRes <-chan kafka.Result, chanError chan<- error, chanKafkaExited <-chan struct{}, mc metrics.Container) {
	collectKafkaErrors := true
	for collectKafkaErrors {
		select {
		case res := <-chanKafkaRes:
			if res.ItemContext != "" {
				var errM error
				errM = mc.IncrementMetric(res.ItemContext, metrics.MetricTypeTotal)
				// in case metric is not available - report error but don't stop the app
				if errM != nil {
					chanError <- errM
				}
				if res.Err != nil {
					chanError <- res.Err
					errM = mc.IncrementMetric(res.ItemContext, metrics.MetricTypeFailed)
				} else {
					errM = mc.IncrementMetric(res.ItemContext, metrics.MetricTypeSucceeded)
				}
				// in case metric is not available - report error but don't stop the app
				if errM != nil {
					chanError <- errM
				}
			}
		case <-chanKafkaExited:
			collectKafkaErrors = false
		}
	}
}

func processErrors(ctx context.Context, chanError <-chan error) {
	continueLoop := true
	for continueLoop {
		select {
		case err := <-chanError:
			//when channel closing we start to always pick this option as default one
			// but this does not mean that error happenned
			if err != nil {
				log.Println(fmt.Errorf("got the following error in app: %w", err))
			}
		case <-ctx.Done():
			continueLoop = false
		}
	}
}

// we could not put processing of these errors into common loop as it is leads to inconsistancy.
// if we close one channel which produce errors - we will be always hitting those case
// and instead of sleeping - goroutine will be contniously doing something.
func redirectMetricsErrorsToErrors(chanMetricsErr <-chan error, chanError chan<- error, chanMetricsExit <-chan struct{}) {
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
	runLoop := true     // use to break app execution
	done := make(chan struct{})
	defer close(done)
	// handle error situation - breaks execution of tool
	errChan := make(chan error) //make it bufferred to not block execution
	defer close(errChan)
	var err error
	for {
		select {
		case <-chanCloseApp:
			errs = append(errs, fmt.Errorf("got termination signal. Exiting"))
			runLoop = false
		case err = <-errChan:
			if err != nil {
				errs = append(errs, err)
			}
			runLoop = false
		// when processing of all feeds done - this channel will be triggered
		case <-done:
			processing = false
		case <-t.C:
			//do not run next round if we already processing feeds or error happenned
			if !processing && runLoop {
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
		// cloase app if got ctrl-break or err
		if !processing && !runLoop {
			break
		}
	}
	return errs
}

func runOnce(feeds []*url.URL, chanKafkaItem chan<- kafka.Itemer, mg MetricsGetter) []error {
	// consider errChan to be notication of finishing processing
	// if succeded - return nil
	// on error return struct with error
	errChan := make(chan error)
	defer close(errChan)
	exitChan := make(chan struct{})
	for _, u := range feeds {
		go func(u *url.URL) {
			//create stream from response to save some memory and speedup processing
			readCloser, err := provider.CreateStream(u)
			if err != nil {
				errChan <- fmt.Errorf("Failed to get stream: %w", err)
				//there is no sense to continue
				close(exitChan)
				return
			}
			m, err := mg.GetMetric(u.String(), "feed")
			// in case metric is not available - report error but don't stop the app
			if err != nil {
				errChan <- fmt.Errorf("Failed to get metric: %w", err)
			} else {
				m.Add(1)
				defer m.Add(-1)
			}

			chanItemProducer, chanProducerError := parser.ProcessFeed(readCloser)
			go func() {
				defer readCloser.Close()
				runLoop := true
				for runLoop {
					select {
					case item := <-chanItemProducer:
						if item.ID != "" {
							topics := []string{kafka.TopicShopItems}
							if !item.HeurekaCPC.Equal(decimal.Zero) {
								topics = append(topics, kafka.TopicShopItemsBidding)
							}
							chanKafkaItem <- appItem{shopItem: item, feed: u.String(), topics: topics}
						}
					case err := <-chanProducerError:
						if err != nil {
							errChan <- fmt.Errorf("Failed to process feed '%s' because of %w", u.String(), err)
						} else {
							errChan <- nil
						}
						close(exitChan)
						runLoop = false
					}
				}
			}()
		}(u)
	}
	//block execution until all goroutines will be finished
	errs := make([]error, 0, 0)
	runLoop := true
	for runLoop {
		select {
		case err := <-errChan:
			if err != nil {
				errs = append(errs, err)
			}
		case <-exitChan:
			runLoop = false
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
