package models

type Order struct {
	ID       int `json:"id"`
	Quantity int `json:"quantity"`
}

type Notification struct {
	ID      int  `json:"id"`
	Payment bool `json:"payment"`
}

type Payment struct {
	ID            int    `json:"id"`
	Message       string `json:"message"`
	PaymentStatus bool   `json:"payment_status"`
}
