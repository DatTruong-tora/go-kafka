package models

type Order struct {
	OrderID      string `json:"order_id"`
	CustomerName string `json:"customer_name"`
	PizzaType    string `json:"pizza_type"`
	IsPriority   bool   `json:"is_priority"`
}
