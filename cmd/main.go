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

	"github.com/davecgh/go-spew/spew"
	"github.com/grubastik/feeddo/cmd/heureka"
	"github.com/jessevdk/go-flags"
	"github.com/shopspring/decimal"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	feeds, kafkaURL, err := parseArgs()
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to parse flags: %v", err))
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaURL})
	if err != nil {
		log.Fatal(fmt.Errorf("Unable to init connection to Kafka: %v", err))
	}

	defer p.Close()

	for _, url := range feeds {
		//create stream from response to save some memory and speedup processing
		var readCloser io.ReadCloser
		if url.Scheme == "file" {
			readCloser, err = os.Open(url.Hostname() + url.EscapedPath())
		} else {
			resp, err := http.Get(url.String())
			if err == nil && resp.Body != nil {
				readCloser = resp.Body
			}
		}
		if err != nil {
			log.Fatal(fmt.Errorf("Unable to read file `%v` because of %v", url, err))
		}
		defer readCloser.Close()
		// try to unmarshal stream.
		// If this stream is not represent expected schema - result will be empty.
		d := xml.NewDecoder(readCloser)
		item := heureka.Item{}
		for {
			var token xml.Token
			token, err = d.Token()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(fmt.Errorf("Failed to unmarshal xml: %v", err))
			}
			switch startElem := token.(type) {
			case xml.StartElement:
				if startElem.Name.Local == "SHOPITEM" {
					err = d.DecodeElement(&item, &startElem)
					if err != nil && err != io.EOF {
						log.Fatal(fmt.Errorf("Failed to unmarshal xml: %v", err))
					}
					message, err := json.Marshal(item)
					if err != nil {
						log.Fatal(fmt.Errorf("Failed to marshal json: %v", err))
					}
					// Produce messages to topic (asynchronously)
					topic := "shop_items"

					deliveryChan := make(chan kafka.Event, 1000)
					defer close(deliveryChan)
					err = p.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
						Value:          []byte(message),
					},
						deliveryChan,
					)

					e := <-deliveryChan
					m := e.(*kafka.Message)
					if m.TopicPartition.Error != nil {
						log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
					} else {
						log.Printf("Delivered message to topic %s [%d] at offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
					if !item.HeurekaCPC.Equal(decimal.Zero) {
						topic := "shop_items_bidding"

						deliveryBiddingChan := make(chan kafka.Event, 1000)
						defer close(deliveryBiddingChan)
						err = p.Produce(&kafka.Message{
							TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
							Value:          []byte(message),
						},
							deliveryBiddingChan,
						)

						e := <-deliveryBiddingChan
						m := e.(*kafka.Message)
						if m.TopicPartition.Error != nil {
							log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
						} else {
							log.Printf("Delivered message to topic %s [%d] at offset %v\n",
								*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
						}
					}
				}
			default:
			}
			if err == io.EOF {
				spew.Dump("Exit loop")
				break
			}
		}
	}
}

func parseArgs() ([]*url.URL, string, error) {
	var opts struct {
		// list of feeds' urls
		URLs     []string `short:"f" long:"feedUrl" description:"Provide url to feeds. Can beused multiple times" required:"true"`
		KafkaURL string   `short:"k" long:"kafkaUrl" description:"Url to connect to kafka" required:"true"`
	}
	parser := flags.NewParser(&opts, flags.PassDoubleDash|flags.IgnoreUnknown)
	_, err := parser.Parse()
	if err != nil {
		return nil, "", fmt.Errorf("Unable to parse flags: %v", err.Error())
	}
	if len(opts.URLs) == 0 {
		return nil, "", fmt.Errorf("List of feed URLs was not provided")
	}
	feeds := []*url.URL{}
	for _, u := range opts.URLs {
		url, err := url.Parse(u)
		if err != nil {
			return nil, "", fmt.Errorf("Unable to parse feed url '%s' because of %v", u, err.Error())
		}
		feeds = append(feeds, url)
	}
	if opts.KafkaURL == "" {
		return nil, "", fmt.Errorf("Kafka url was not provided")
	}

	return feeds, opts.KafkaURL, nil
}
