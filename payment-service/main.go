package payment

import (
	"kafka-microservices/db"
	"kafka-microservices/models"
	"log"
)

func DoPayment(data models.Payment) bool {

	log.Println("Inside the Do Payment function")

	db, err := db.ConnectToDB()
	if err != nil {
		log.Println("error in connecting to database ", err)
		return false
	}

	

	sql := `insert into payment(userid,message,payment_status) values ($1,$2,$3)`
	err = db.QueryRow(sql, data.ID, data.Message, data.PaymentStatus).Err()
	if err != nil {
		log.Println("error in inserting to payment table ",err)
		return false

	}
	log.Println("Inserted to payment table ")

	return true
}
