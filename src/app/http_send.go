package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const CodeUrl = "http://192.168.123.120:8000/code" // адрес канального уровня

func SendSegment(body Segment) {
	reqBody, _ := json.Marshal(body)

	req, _ := http.NewRequest("POST", CodeUrl, bytes.NewBuffer(reqBody))
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return
	}

	defer resp.Body.Close()
}

const SegmentSize = 100

type SendRequest struct {
	Id       int       `json:"id,omitempty"`
	Username string    `json:"username"`
	Text     string    `json:"data"`
	SendTime time.Time `json:"send_time"`
}

func HandleSend(w http.ResponseWriter, r *http.Request) {
	// читаем тело запроса - сообщение
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// парсим сообщение в структуру
	message := SendRequest{}
	if err = json.Unmarshal(body, &message); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// сразу отвечаем прикладному уровню 200 ОК - мы приняли работу
	w.WriteHeader(http.StatusOK)

	// разбиваем текст сообщения на сегменты
	segments := SplitMessage(message.Text, SegmentSize)
	total := len(segments)

	// в цикле отправляем сегменты на канальный уровень
	for i, segment := range segments {
		payload := Segment{
			SegmentNumber:  i + 1,
			TotalSegments:  total,
			Username:       message.Username,
			SendTime:       message.SendTime,
			SegmentPayload: segment,
		}
		go SendSegment(payload) // запускаем горутину с отправкой на канальный уровень, не будем дожидаться результата ее выполнения
		fmt.Printf("sent segment: %+v\n", payload)
	}
}
