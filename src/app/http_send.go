package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Сообщение от прикладного уровня
type SendRequest struct {
	Sender		string		`json:"sender"`
	SendTime	time.Time	`json:"send_time"`
	Payload		string		`json:"data"`
}

// Сообщение канальному уровня
type Segment struct {
	SegmentNumber	int			`json:"segment_number"`
	TotalSegments	int			`json:"total_segments"`
	Sender			string		`json:"sender"`
	SendTime		time.Time	`json:"send_time"`
	SegmentPayload	string		`json:"payload"`
}

// Функция для разделения сообщения на сегменты
func splitSegment(payload string, segmentSize int) []string {
	result := make([]string, 0)

	length := len(payload) // длина сообщения в байтах
	segmentCount := (length + segmentSize - 1) / segmentSize

	for i := 0; i < segmentCount; i++ {
		result = append(result, payload[i*segmentSize:min((i+1)*segmentSize, length)])
	}

	return result
}

// Функция для отправки сегмента на канальный уровень
func sendSegment(url string, body Segment, wg *sync.WaitGroup, errors chan error) {
    defer wg.Done()

    // Сериализация структуры в JSON
    payload, err := json.Marshal(body)
    if err != nil {
        errors <- fmt.Errorf("ошибка сериализации сегмента: %v", err)
        return
    }

    log.Printf("[<-] Отправка сегмента: %s", string(payload))

    // Отправляем POST-запрос
    resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
    if err != nil {
        errors <- fmt.Errorf("ошибка отправки запроса: %v", err)
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode == http.StatusOK {
        log.Printf("Сегмент %v отправлен успешно, статус: %s", body, resp.Status)
        return
    }

    respBody, err := io.ReadAll(resp.Body)
    if err != nil {
        errors <- fmt.Errorf("сегмент %d не отправлен: %s, ошибка чтения ответа: %v", body.SegmentNumber, resp.Status, err)
        return
    }

    var errResp struct {
        Error string `json:"error"`
    }
    msg := string(respBody)
    if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
        msg = errResp.Error
    }
    errors <- fmt.Errorf("сегмент %d не отправлен: %s, ошибка: %s", body.SegmentNumber, resp.Status, msg)
}

// Обработчик POST-запросов от прикладного уровня
func HandleSend(w http.ResponseWriter, r *http.Request) {
    defer r.Body.Close()
    log.Printf("Получен запрос на /send, метод: %s, URL: %s", r.Method, r.URL)

    // Чтение тела запроса
    req, err := io.ReadAll(r.Body)
    if err != nil {
        http.Error(w, "Ошибка чтения тела", http.StatusBadRequest)
        log.Printf("Ошибка чтения тела запроса: %v", err)
        return
    }

    // Парсим сообщение в структуру
    var message SendRequest
    err = json.Unmarshal(req, &message)
    if err != nil || message.Sender == "" || message.Payload == "" || message.SendTime.IsZero() {
        http.Error(w, "Ошибка парсинга тела запроса", http.StatusBadRequest)
        log.Printf("Ошибка парсинга запроса: %v", err)
        return
    }
    log.Printf("[->] Полученные данные от прикладного уровня: %+v", message)

    // Разделяем на сегменты
    payloadSegments := splitSegment(message.Payload, SegmentSize)
    totalSegments := len(payloadSegments)

    var wg sync.WaitGroup
    errors := make(chan error, totalSegments)

    // Отправляем каждый сегмент асинхронно
    for i, payload := range payloadSegments {
        segment := Segment{
            SegmentNumber:  i + 1,
            TotalSegments:  totalSegments,
            Sender:         message.Sender,
            SendTime:       message.SendTime,
            SegmentPayload: payload,
        }

        wg.Add(1)
        go sendSegment(urlChannelLevel, segment, &wg, errors)
    }

    wg.Wait()
    close(errors)

    var errorMessages []string
    for err := range errors {
        log.Printf("Ошибка при отправке сегмента: %v", err)
        errorMessages = append(errorMessages, err.Error())
    }

    if len(errorMessages) == 0 {
        msg := "Все сегменты успешно отправлены на канальный уровень"
        w.WriteHeader(http.StatusOK)
        fmt.Fprintln(w, msg)
        log.Println(msg)
    } else {
        http.Error(w, strings.Join(errorMessages, "\n"), http.StatusInternalServerError)
    }

}