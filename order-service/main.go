package main

import (
	"kafka-microservices/producer"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	Producer producer.ProducerImpl
}

func NewHandler(producer producer.Producer) *Handler {
	return &Handler{
		Producer: &producer,
	}
}

func main() {
	producer := producer.NewProducer()
	handler := NewHandler(*producer)
	router := gin.Default()
	router.POST("/order", handler.ReceiveOrder)
	router.Run(":8080")
}
