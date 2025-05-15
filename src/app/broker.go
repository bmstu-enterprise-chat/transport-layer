package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// Структура для финального сообщения на прикладной уровень
type OutputMessage struct {
	Sender   string    `json:"sender"`
	SendTime time.Time `json:"send_time"`
	Payload  string    `json:"payload"`             // Собранная полезная нагрузка
	Error    bool      `json:"error,omitempty"`     // Признак ошибки (опускается, если false)
	ErrorMsg string    `json:"error_msg,omitempty"` // Сообщение об ошибке (опускается, если пустое)
}

// Структура для хранения состояния сборки одного логического сообщения
type MessageReassemblyState struct {
	Segments              map[int]Segment // Хранит полученные сегменты по их номеру
	TotalSegmentsExpected int             // Общее количество ожидаемых сегментов
	LastSegmentArrivalTime time.Time       // Время поступления последнего сегмента для этого сообщения
	Sender                string
	SendTime              time.Time
}

// Коллекция незавершенных сообщений, ожидающих сегменты
// Ключ - уникальный идентификатор сообщения (например, Sender + SendTime)
var (
	inFlightMessages map[string]*MessageReassemblyState
	inFlightMutex    sync.Mutex // Мьютекс для защиты доступа к map
)

// ReassemblyGoroutine - Горутина для сборки сегментов из Kafka.
func ReassemblyGoroutine(ctx context.Context) {
	log.Println("Запуск горутины сборки сегментов...")

	// Создание Kafka consumer с настройками
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     KafkaAddr,
		"group.id":              "segment-reassembly-group",
		"auto.offset.reset":     "earliest",
		"enable.auto.commit":    false,
		"session.timeout.ms":    10000,
		"heartbeat.interval.ms": 3000,
	})
	if err != nil {
		log.Fatalf("Не удалось создать Kafka consumer: %v", err)
	}
	// Закрытие consumer будет вызываться при завершении горутины

	// Подписка на Kafka топик
	err = consumer.SubscribeTopics([]string{KafkaTopic}, nil)
	if err != nil {
		log.Fatalf("Не удалось подписаться на топик %s: %v", KafkaTopic, err)
	}
	log.Printf("Kafka consumer подписан на топик: %s", KafkaTopic)

	// Инициализация коллекции незавершенных сообщений
	inFlightMessages = make(map[string]*MessageReassemblyState)

	// Настройка таймера для периодической проверки незавершенных сообщений
	ticker := time.NewTicker(BuildInterval)
	defer ticker.Stop()

	log.Println("Горутина сборки сегментов запущена.")

	// Основной цикл обработки событий и сообщений
	for {
		select {
		case <-ctx.Done():
			// Завершение горутины при получении сигнала через контекст
			log.Println("Получен сигнал на завершение горутины сборки сегментов.")
			consumer.Close() // Закрытие Kafka consumer
			log.Println("Kafka consumer закрыт.")
			return

		case ev := <-consumer.Events(): // Обработка событий Kafka
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				log.Printf("Получены назначения разделов: %v", e)
			case kafka.RevokedPartitions:
				log.Printf("Отзыв разделов: %v", e)
			case kafka.PartitionEOF:
				log.Printf("Достигнут конец раздела %v", e)
			case kafka.Error:
				if e.IsFatal() {
					log.Fatalf("Фатальная ошибка Kafka consumer: %v", e)
				} else {
					log.Printf("Нефатальная ошибка Kafka consumer: %v", e)
				}
			}

		case <-ticker.C:
			// Периодическая проверка таймаутов и завершенных сообщений
			now := time.Now()
			keysToSend := []string{}
			inFlightMutex.Lock()

			// Проверка незавершенных сообщений
			for key, state := range inFlightMessages {
				if len(state.Segments) == state.TotalSegmentsExpected {
					log.Printf("Сообщение по ключу '%s' полностью собрано.", key)
					outputSuccessMessage := formatOutputMessage(state, true)
					// Отправляем успешное сообщение
					go sendToApplLevel(outputSuccessMessage)
					keysToSend = append(keysToSend, key)
				} else if now.Sub(state.LastSegmentArrivalTime) > MaxInactivityCycles {
					log.Printf("Сообщение по ключу '%s' истек таймаут", key)
					outputErrMessage := formatOutputMessage(state, false)
					// Отправляем сообщение об ошибке
					go sendToApplLevel(outputErrMessage)
					keysToSend = append(keysToSend, key)
				}
			}

			// Удаление завершенных сообщений
			for _, key := range keysToSend {
				delete(inFlightMessages, key)
				log.Printf("Сообщение по ключу '%s' удалено из коллекции.", key)
			}
			inFlightMutex.Unlock()

		default:
			// Чтение сообщений из Kafka
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err == nil {
				var segment Segment
				err = json.Unmarshal(msg.Value, &segment)
				if err != nil {
					log.Printf("Ошибка при десериализации сегмента: %v", err)
					continue
				}

				log.Printf("Обработка сегмента %d/%d: Отправитель='%s', Время='%s'", segment.SegmentNumber, segment.TotalSegments, segment.Sender, segment.SendTime.Format(time.RFC3339))

				// Формирование уникального ключа для сообщения
				messageKey := fmt.Sprintf("%s_%s", segment.Sender, segment.SendTime.Format(time.RFC3339Nano))

				inFlightMutex.Lock()

				// Обработка состояния сборки для сообщения
				state, exists := inFlightMessages[messageKey]
				if !exists {
					state = &MessageReassemblyState{
						Segments: make(map[int]Segment),
						TotalSegmentsExpected: segment.TotalSegments,
						Sender:                segment.Sender,
						SendTime:              segment.SendTime,
					}
					inFlightMessages[messageKey] = state
				} else {
					if state.TotalSegmentsExpected != segment.TotalSegments || state.Sender != segment.Sender || !state.SendTime.Equal(segment.SendTime) {
						log.Printf("Несоответствие метаданных сегмента для ключа '%s'", messageKey)
						inFlightMutex.Unlock()
						continue
					}
				}

				// Добавление нового сегмента
				if _, received := state.Segments[segment.SegmentNumber]; !received {
					state.Segments[segment.SegmentNumber] = segment
					state.LastSegmentArrivalTime = time.Now()
					log.Printf("Добавлен сегмент %d/%d для сообщения '%s'", segment.SegmentNumber, state.TotalSegmentsExpected, messageKey)
				} else {
					log.Printf("Получен дубликат сегмента %d для сообщения '%s'", segment.SegmentNumber, messageKey)
				}

				inFlightMutex.Unlock()

				// Коммит оффсета после обработки сегмента
				_, commitErr := consumer.CommitMessage(msg)
				if commitErr != nil {
					log.Printf("Ошибка при коммите оффсета: %v", commitErr)
				}

			} else if err.(kafka.Error).IsFatal() {
				log.Fatalf("Фатальная ошибка Kafka consumer: %v", err)
			}
		}
	}
}

// formatOutputMessage - Вспомогательная функция для форматирования финального сообщения OutputMessage
func formatOutputMessage(state *MessageReassemblyState, success bool) OutputMessage {
	output := OutputMessage{
		Sender: state.Sender,
		SendTime: state.SendTime,
	}

	if success {
		// Собираем полезную нагрузку из сегментов
		payloadBuilder := strings.Builder{}
		for i := 1; i <= state.TotalSegmentsExpected; i++ {
			segment, ok := state.Segments[i]
			if ok {
				payloadBuilder.WriteString(segment.SegmentPayload)
			} else {
				// Это случай ошибки сборки, хотя мы форматируем как "успех"
				// В реальном приложении, возможно, стоило бы пометить это как ошибку или логировать
				log.Printf("Внимание: Отсутствует сегмент %d для сообщения '%s' при сборке успешной полезной нагрузки.", i, state.Sender)
			}
		}
		output.Payload = payloadBuilder.String()
		output.Error = false
	} else {
		// Сообщение об ошибке
		output.Error = true
		output.ErrorMsg = fmt.Sprintf("Истек таймаут сообщения. Ожидалось %d сегментов, получено %d.", state.TotalSegmentsExpected, len(state.Segments))
		output.Payload = "" // Полезная нагрузка отсутствует при ошибке
	}

	return output
}

// sendToApplLevel отправляет собранное сообщение POST запросом на прикладной уровень.
func sendToApplLevel(message OutputMessage) {
	log.Printf("[<-] Сообщение, отправляемое на прикладной уровень: %+v", message)

	jsonData, err := json.Marshal(message)
	if err != nil {
		log.Printf("Ошибка при маршалинге сообщения для отправки: %v", err)
		return
	}

	req, err := http.NewRequest("POST", urlApplLevel, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Ошибка при создании POST запроса: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second} // Ограничиваем время ожидания ответа

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Ошибка при отправке POST запроса на %s: %v", urlApplLevel, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Получен некорректный статус ответа от %s: %d", urlApplLevel, resp.StatusCode)
		// Можно прочитать тело ответа для получения подробностей об ошибке
	} else {
		log.Printf("Сообщение успешно отправлено на %s", urlApplLevel)
	}
}