package kafka

import (
	"context"
	"fmt"
	"sync"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	// TopicShopItems all items will be sent to this topic
	TopicShopItems = "shop_items"
	// TopicShopItemsBidding will recieve only items with bidding set and greater than zero
	TopicShopItemsBidding = "shop_items_bidding"
	// KafkaAddressCtxKey context key for kafka address
	KafkaAddressCtxKey = "addressKafka"
	// MaxProducersCtxKey context key for max numbers of producers
	MaxProducersCtxKey = "kafkaMaxProducers"
)

// ProducerProvider for kafka topics
type ProducerProvider interface {
	Produce(*kafka.Message, chan kafka.Event) error
	Close()
}

// Producer for kafka topics
type Producer struct {
	kafkaProducer ProducerProvider
	ctx           context.Context
}

// Result indicates message processing status
// on success - err will be nil
// on error - err will contain corresponding error
type Result struct {
	ItemContext string
	ItemID      string
	Err         error
}

// Itemer defines interface for processed entities
type Itemer interface {
	GetContext() string
	GetID() string
	Marshal() ([]byte, error)
	Topics() []string
}

// NewKafkaProducer returned configured kafka producer
func NewKafkaProducer(ctx context.Context) (*Producer, error) {
	addr, err := getAddressFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("Unable to get Kafka address from context: %w", err)
	}
	// all options could be found here https://docs.confluent.io/5.5.0/clients/librdkafka/md_CONFIGURATION.html
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":              addr,
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
		return nil, fmt.Errorf("Unable to init connection to Kafka: %w", err)
	}
	return &Producer{kafkaProducer: p, ctx: ctx}, nil
}

// CreateProducersPool creates pool of goroutines which will handle populating items to kafka
func (p *Producer) CreateProducersPool(chanItem <-chan Itemer) (<-chan Result, <-chan struct{}) {
	chanProducersExited := make(chan struct{})
	chanRes := make(chan Result, 1)
	var maxProducers int
	var ok bool
	if maxProducers, ok = p.ctx.Value(MaxProducersCtxKey).(int); !ok {
		defer func() {
			chanRes <- Result{Err: fmt.Errorf("'%s' key should be set in context and has int value type", MaxProducersCtxKey)}
			// we did not start goroutine yet.
			// it is required to close both channels here
			close(chanRes)
			close(chanProducersExited)
		}()
		return chanRes, chanProducersExited
	}
	go func() {
		defer func() {
			close(chanRes)
			close(chanProducersExited)
		}()
		wg := sync.WaitGroup{}
		for i := 0; i < maxProducers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				continueLoop := true
				for continueLoop {
					select {
					// if this channel will be closed - we will go here with default value for item
					case item := <-chanItem:
						// all items should belong to some context
						if item.GetContext() != "" {
							chanRes <- p.putItemToKafka(item)
						}
					case <-p.ctx.Done():
						continueLoop = false
					}
				}
			}()
		}
		wg.Wait()
	}()
	return chanRes, chanProducersExited
}

func (p *Producer) putItemToKafka(item Itemer) Result {
	res := Result{ItemID: item.GetID(), ItemContext: item.GetContext()}
	message, err := item.Marshal()
	if err != nil {
		res.Err = fmt.Errorf("Failed to marshal json: %w", err)
		return res
	}
	// Produce messages to topic (asynchronously)
	for _, topic := range item.Topics() {
		err = p.sendMessageToKafka(topic, message)
		if err != nil {
			res.Err = fmt.Errorf("Failed to send message to topic %s because of: %w", topic, err)
			return res
		}
	}
	return res
}

func (p *Producer) sendMessageToKafka(topic string, m []byte) error {
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte(m),
	}
	err := p.kafkaProducer.Produce(km, deliveryChan)
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

func getAddressFromContext(ctx context.Context) (string, error) {
	addrRaw := ctx.Value(KafkaAddressCtxKey)
	if addrRaw == nil {
		return "", fmt.Errorf("Provided context does not contain key '%s'", KafkaAddressCtxKey)
	}
	addr, ok := addrRaw.(string)
	if !ok || addr == "" {
		return "", fmt.Errorf("Provided context does not contain string value in key '%s'", KafkaAddressCtxKey)
	}
	return addr, nil
}

// Close wrapper for producer provider
func (p *Producer) Close() {
	p.kafkaProducer.Close()
}
