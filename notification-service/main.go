package notification

import (
	"kafka-microservices/models"
	"log"
)

func ShowNotification(notData models.Notification){
	log.Println("Inside the ShowNotification service")

	log.Println("UserID is ",notData.ID)
	log.Println("Payment status is ",notData.Payment)
}