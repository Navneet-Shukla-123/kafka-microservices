package main

import (
	"kafka-microservices/db"
	"kafka-microservices/producer"
	"log"

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
	_, err := db.ConnectToDB()
	if err != nil {
		log.Println("error in connecting to database ", err)

	}

	log.Println("Connected to DB")
	producer := producer.NewProducer()
	handler := NewHandler(*producer)
	router := gin.Default()
	router.POST("/order", handler.ReceiveOrder)
	router.Run(":8080")
}
