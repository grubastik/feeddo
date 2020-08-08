package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func TestRunServerContextError(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		err  string
	}{
		{"No key", context.Background(),
			fmt.Sprintf("Unable to get Kafka address from context: Provided context does not contain key '%s'", KafkaAddressCtxKey)},
		{"No value", context.WithValue(context.Background(), KafkaAddressCtxKey, nil),
			fmt.Sprintf("Unable to get Kafka address from context: Provided context does not contain key '%s'", KafkaAddressCtxKey)},
		{"Empty string", context.WithValue(context.Background(), KafkaAddressCtxKey, ""),
			fmt.Sprintf("Unable to get Kafka address from context: Provided context does not contain string value in key '%s'", KafkaAddressCtxKey)}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKafkaProducer(tt.ctx)
			require.Error(t, err)
			assert.Equal(t, tt.err, err.Error())
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
func (pp producerSuccess) Close() {}

type producerError struct{}

func (pp producerError) Produce(m *kafka.Message, c chan kafka.Event) error {
	return errors.New("test error")
}
func (pp producerError) Close() {}

type producerChannelError struct{}

func (pp producerChannelError) Produce(m *kafka.Message, c chan kafka.Event) error {
	go func() {
		km := &kafka.Message{TopicPartition: kafka.TopicPartition{Error: errors.New("Test channel error")}}
		c <- km
	}()
	return nil
}
func (pp producerChannelError) Close() {}

func TestSendMessageToKafka(t *testing.T) {
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
			producer: Producer{kafkaProducer: producerError{}, ctx: nil},
			err:      "Send message to kafka failed because of test error",
		},
		{
			name:     "Producer failed to deliver message to kafka",
			topic:    "test",
			message:  []byte("test"),
			producer: Producer{producerChannelError{}, nil},
			err:      "Delivery to kafka failed: Test channel error",
		},
		{
			name:     "happy path",
			topic:    "test",
			message:  []byte("test"),
			producer: Producer{producerSuccess{}, nil},
			err:      "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.producer.sendMessageToKafka(tt.topic, tt.message)
			if tt.err != "" {
				require.Error(t, err)
				assert.Equal(t, tt.err, err.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type ItemTest struct{}

func (i ItemTest) GetContext() string       { return "testContext" }
func (i ItemTest) GetID() string            { return "testID" }
func (i ItemTest) Marshal() ([]byte, error) { return []byte("test bytes"), nil }
func (i ItemTest) Topics() []string         { return []string{TopicShopItems} }

type ItemMarshalErrorTest struct{ ItemTest }

func (i ItemMarshalErrorTest) Marshal() ([]byte, error) { return nil, fmt.Errorf("Test error") }

func TestPutItemToKafka(t *testing.T) {
	tests := []struct {
		name     string
		producer Producer
		item     Itemer
		err      string
	}{
		{
			"Item Marshal Error", Producer{kafkaProducer: producerError{}, ctx: nil}, ItemMarshalErrorTest{}, "Failed to marshal json: Test error",
		},
		{
			"Item Producer Error", Producer{kafkaProducer: producerError{}, ctx: nil}, ItemTest{}, "Failed to send message to topic shop_items because of: Send message to kafka failed because of test error",
		},
		{
			"Item Producer Error", Producer{kafkaProducer: producerSuccess{}, ctx: nil}, ItemTest{}, "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := tt.producer.putItemToKafka(tt.item)
			if tt.err != "" {
				require.Error(t, r.Err)
				assert.Equal(t, tt.err, r.Err.Error())
			} else {
				require.NoError(t, r.Err)
				assert.Equal(t, tt.item.GetContext(), r.ItemContext)
				assert.Equal(t, tt.item.GetID(), r.ItemID)
			}
		})
	}
}

func TestCreateProducersPool(t *testing.T) {
	tests := []struct {
		name     string
		producer Producer
		item     []Itemer
		err      string
	}{
		{
			"Context No key", Producer{kafkaProducer: producerSuccess{}, ctx: context.Background()}, []Itemer{ItemTest{}}, "'kafkaMaxProducers' key should be set in context and has int value type",
		},
		{
			"Happy path single item",
			Producer{kafkaProducer: producerSuccess{}, ctx: context.WithValue(context.Background(), MaxProducersCtxKey, 1)},
			[]Itemer{ItemTest{}},
			"",
		},
		{
			"Happy path multiple items",
			Producer{kafkaProducer: producerSuccess{}, ctx: context.WithValue(context.Background(), MaxProducersCtxKey, 2)},
			[]Itemer{ItemTest{}, ItemTest{}, ItemTest{}, ItemTest{}, ItemTest{}, ItemTest{}, ItemTest{}, ItemTest{}},
			"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chanItem := make(chan Itemer)
			defer close(chanItem)
			ctx, cancelFunc := context.WithCancel(tt.producer.ctx)
			tt.producer.ctx = ctx
			resChan, closeChan := tt.producer.CreateProducersPool(chanItem)
			if tt.err != "" {
				defer cancelFunc()
				res := <-resChan
				<-closeChan
				require.Error(t, res.Err)
				assert.Equal(t, tt.err, res.Err.Error())
			} else {
				wg := sync.WaitGroup{}
				wg.Add(2)
				go func() {
					defer wg.Done()
					for _, i := range tt.item {
						chanItem <- i
					}
				}()
				go func() {
					defer wg.Done()
					for _, i := range tt.item {
						res := <-resChan
						assert.NoError(t, res.Err)
						if res.Err == nil {
							assert.Equal(t, i.GetContext(), res.ItemContext)
							assert.Equal(t, i.GetID(), res.ItemID)
						}
					}
				}()
				wg.Wait()
				cancelFunc()
				<-closeChan
			}
		})
	}
}
