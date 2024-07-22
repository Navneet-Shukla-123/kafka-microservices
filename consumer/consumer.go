package main

import (
	"context"
	"encoding/json"
	"fmt"
	"kafka-microservices/models"
	"kafka-microservices/notification-service"
	"kafka-microservices/payment-service"
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
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer *Consumer) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		topic := msg.Topic

		value := msg.Value

		if topic == ConsumerTopicOrder {
			log.Println("Topic is ", topic)
			var orderData models.Order

			err := json.Unmarshal(value, &orderData)
			if err != nil {
				log.Println("error in unmarshalling the value ", err)
				continue
			}

			log.Println("value is ", string(value))

			produ, err := producer.NewProducer().SetupProducer()
			if err != nil {
				log.Println("error in setting up the producer ", err)
				continue
			}

			not := models.Notification{
				ID:      orderData.ID,
				Payment: false,
			}

			err = producer.NewProducer().SendKafkaMessage(produ, not, ConsumerTopicNotification)
			if err != nil {
				log.Println("error in sending the notification after order to kafka ", err)
				continue
			}
		} else if topic == ConsumerTopicNotification {
			var notiData models.Notification

			log.Println("Topic is ", topic)
			log.Println("Value is ", string(value))

			err := json.Unmarshal(value, &notiData)
			if err != nil {
				log.Println("error in unmarshalling the notification after order ", err)
				continue
			}

			notification.ShowNotification(notiData)

			if !notiData.Payment {
				var paymentData models.Payment

				paymentData.ID = notiData.ID
				paymentData.Message = "payment is not done yet"
				paymentData.PaymentStatus = false

				produ, err := producer.NewProducer().SetupProducer()
				if err != nil {
					log.Println("error in setting up the producer ", err)
					continue
				}

				err = producer.NewProducer().SendKafkaMessage(produ, paymentData, ConsumerTopicPayment)
				if err != nil {
					log.Println("error in sending the payment to payment topic ", err)
					continue
				}

			}
		} else if topic == ConsumerTopicPayment {
			log.Println("Topic is ", topic)
			log.Println("Value is ", string(value))

			var paymentData models.Payment

			err := json.Unmarshal(value, &paymentData)
			if err != nil {
				log.Println("error in unmarshalling the payment")
				continue
			}

			paymentStatus := payment.DoPayment(paymentData)

			var notiData models.Notification

			notiData.ID = paymentData.ID
			notiData.Payment = paymentStatus
			produ, err := producer.NewProducer().SetupProducer()
			if err != nil {
				log.Println("error in setting up the producer ", err)
				continue
			}

			err = producer.NewProducer().SendKafkaMessage(produ, notiData, ConsumerTopicNotification)
			if err != nil {
				log.Println("error in sending the payment to Notification topic ", err)
				continue
			}

		}

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

func setupConsumerGroup(ctx context.Context) {
	consumerGroup, err := initializeConsumerGroup()
	if err != nil {
		log.Printf("initialization error for consumer group: %v", err)
	}

	defer consumerGroup.Close()

	consumer := &Consumer{}
	topics := []string{
		ConsumerTopicNotification, ConsumerTopicOrder, ConsumerTopicPayment,
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

	ctx, cancel := context.WithCancel(context.Background())
	setupConsumerGroup(ctx)
	defer cancel()

}
