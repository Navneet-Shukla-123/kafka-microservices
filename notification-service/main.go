package main

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-microservices/models"
	"kafka-microservices/producer"
	"log"

	"github.com/IBM/sarama"
)

const (
	ConsumerGroup             = "ecommerce-group"
	ConsumerTopicOrder        = "order"
	ConsumerTopicNotification = "notification"
	ConsumerTopicPayment      = "payment"
	KafkaServerAddress        = "localhost:9092"
)

type Consumer struct {
	Producer producer.ProducerImpl
}

func NewConsumer(producer producer.Producer) *Consumer {
	return &Consumer{
		Producer: &producer,
	}
}



func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer *Consumer) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		topic := msg.Topic

		value := msg.Value

		var data models.Order

		err := json.Unmarshal(value, &data)
		if err != nil {
			log.Printf("error in converting the data of %s topic in notification service \n", topic)
			return err
		}

		var NotificationData models.Notification
		NotificationData.ID = data.ID
		NotificationData.Payment = false

		producer, err := consumer.Producer.SetupProducer()
		if err != nil {
			log.Println("error in setting up the producer for the in notification server")
			return err
		}

		err = consumer.Producer.SendKafkaMessage(producer, NotificationData, ConsumerTopicNotification)
		if err != nil {
			log.Println("error in sending the message from the notification service after receiving the order")
			return err
		}
		log.Println("notification sent after receiving the order")

		sess.MarkMessage(msg, "")
	}
	return nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	consumerGroup, err := sarama.NewConsumerGroup(
		[]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return consumerGroup, nil
}

func SetupConsumerGroup(ctx context.Context) {
	consumerGroup, err := initializeConsumerGroup()
	if err != nil {
		log.Printf("initialization error for consumer group: %v", err)
	}

	defer consumerGroup.Close()

	consumer := &Consumer{}
	topics := []string{
		ConsumerTopicOrder,
	}

	for {
		err = consumerGroup.Consume(ctx, topics, consumer)
		if err != nil {
			log.Printf("error from consumer: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

func main() {

	producer:=producer.NewProducer()

	NewConsumer(*producer)
	ctx, cancel := context.WithCancel(context.Background())
	go SetupConsumerGroup(ctx)
	defer cancel()

}
