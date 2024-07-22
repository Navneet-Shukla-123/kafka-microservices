package payment

import (
	"kafka-microservices/models"
	"log"
)

func DoPayment(data models.Payment) bool {

	log.Println("Inside the Do Payment function")
	return true
}
