package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

func logErrorAndSend(errChan chan<- error, msg string, err error) {
	log.Printf(msg, err)
	errChan <- fmt.Errorf(msg, err)
}

// Функция для продюсера Kafka
func produceSegment(segment Segment, errChan chan<- error) {
	defer close(errChan)
	log.Printf("Горутина продюсера запущена для сегмента #%d", segment.SegmentNumber)

	// Создаем писатель Kafka.
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{KafkaAddr}, // Адрес брокера(ов) Kafka
		Topic:   KafkaTopic,          // Топик Kafka
	})

	// Закрываем писателя при завершении функции
	defer func() {
		if cErr := writer.Close(); cErr != nil {
			log.Printf("Ошибка при закрытии писателя Kafka: %v", cErr)
		} else {
			log.Println("Писатель Kafka успешно закрыт.")
		}
	}()

	// Сериализуем структуру Segment в JSON формат.
	segmentBytes, err := json.Marshal(segment)
	if err != nil {
		logErrorAndSend(errChan, "Ошибка при сериализации сегмента в JSON: %v", err)
		return
	}

	// Создаем сообщение Kafka.
	msg := kafka.Message{
		Value: segmentBytes,
	}

	// Контекст для операции записи, позволяющий установить таймаут.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // Таймаут 15 секунд на запись
	defer cancel() // Освобождаем ресурсы контекста

	// Отправляем сообщение в Kafka.
	err = writer.WriteMessages(ctx, msg)
	if err != nil {
		logErrorAndSend(errChan, "Ошибка при записи сообщения в Kafka: %v", err)
		return
	}

	log.Println("Сегмент успешно записан в Kafka")
	errChan <- nil
}

// Обработчик POST-запросов от канального уровня
func HandleTransfer(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	log.Printf("Получен запрос на /transfer, метод: %s, URL: %s", r.Method, r.URL)

	// Чтение тела запроса
	req, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Ошибка чтения тела", http.StatusBadRequest)
		log.Printf("Ошибка чтения тела запроса: %v", err)
		return
	}

	// Парсим сообщение в структуру
	var segment Segment
	err = json.Unmarshal(req, &segment)
	if err != nil || segment.Sender == "" || segment.SegmentPayload == "" || segment.SegmentNumber == 0 || segment.TotalSegments == 0 || segment.SendTime.IsZero() {
		http.Error(w, "Ошибка парсинга тела запроса", http.StatusBadRequest)
		log.Printf("Ошибка парсинга запроса: %v", err)
		return
	}

	log.Printf("[->] Полученные данные от канального уровня: %+v", segment)

	// Создаем канал для получения ошибки от горутины продюсера.
	errChan := make(chan error, 1)

	go produceSegment(segment, errChan)

	producerErr := <-errChan

	// Проверяем, была ли ошибка в горутине продюсера
	if producerErr != nil {
		log.Printf("Ошибка от продюсера Kafka: %v", producerErr)
		http.Error(w, fmt.Sprintf("Ошибка записи сегмента в брокер Kafka: %v", producerErr), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Сегмент принят и успешно отправлен в Kafka")
	log.Println("Сегмент принят на транспортном уровне и успешно отправлен в Kafka")
}
