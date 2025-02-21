package app

import (
	"bytes"
	"encoding/json"
	"net/http"
)

const ReceiveUrl = "http://192.168.123.140:3000/receive" // адрес websocket-сервера прикладного уровня

func SendReceiveRequest(body ReceiveRequest) {
	reqBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", ReceiveUrl, bytes.NewBuffer(reqBody))
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close()
}
