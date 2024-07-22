package producer

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

type Producer struct {
}

type ProducerImpl interface {
	SendKafkaMessage(producer sarama.SyncProducer, data interface{}, topic string) error
	SetupProducer() (sarama.SyncProducer, error)
}

func NewProducer() *Producer {
	return &Producer{}
}

const (
	KafkaServerAddress     = "localhost:9092"
	KafkaTopicOrder        = "order"
	KafkaTopicNotification = "notification"
	KafkaTopicPayment      = "payment"
)

func (p *Producer) SendKafkaMessage(producer sarama.SyncProducer, data interface{}, topic string) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("error in marshalling the json for %s topic \n", topic)
		return err
	}

	//log.Println("Json data is ", string(jsonData))
	msg := &sarama.ProducerMessage{
		Topic: topic,
		//Key:   sarama.StringEncoder("number"),
		Value: sarama.StringEncoder(string(jsonData)),
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return err
	}

	return nil
}

func (p *Producer) SetupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress},
		config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}

// func main() {
// 	producer, err := SetupProducer()
// 	if err != nil {
// 		log.Println("error in setting up the server ", err)
// 		return
// 	}

// 	defer producer.Close()

// 	sendMessageFunction(producer)

// }
