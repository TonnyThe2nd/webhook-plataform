package queue

import (
	"encoding/json"
	"fmt"
)

type DeliveryMessage struct {
	DeliveryID string `json:"delivery_id"`
	Attempt    int    `json:"attempt"`
}

func (m DeliveryMessage) Marshal() (string, error) {
	b, err := json.Marshal(m)
	return string(b), err
}

func UnmarshalDelivery(body string) (DeliveryMessage, error) {
	var m DeliveryMessage
	err := json.Unmarshal([]byte(body), &m)
	if err == nil && (m.DeliveryID == "" || m.Attempt < 0) {
		err = fmt.Errorf("delivery_id obrigatório e attempt deve ser não negativo")
	}
	return m, err
}
