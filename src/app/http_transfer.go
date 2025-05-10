package app

import (
	"net/http"
	"log"
	"io"
	"encoding/json"
)

// Обработчик POST-запросов от канального уровня
func HandleTransfer(w http.ResponseWriter, r *http.Request) {
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
    log.Printf("Полученные данные от прикладного уровня: %+v", message)
}