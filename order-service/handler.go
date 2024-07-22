package main

import (
	"kafka-microservices/models"
	"kafka-microservices/producer"
	"net/http"

	"github.com/gin-gonic/gin"
)

func (h *Handler) ReceiveOrder(c *gin.Context) {
	var req models.Order

	err := c.ShouldBindJSON(&req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "error reading the request body",
		})
		return
	}

	producers, err := h.Producer.SetupProducer()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "error in setting up the producer",
		})
		return
	}
	err = h.Producer.SendKafkaMessage(producers, req, producer.KafkaTopicOrder)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": "error sending data to kafka",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "order sent to kafka",
	})

}
