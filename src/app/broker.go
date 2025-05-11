package app

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
	"strings"

	kafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// Структура для финального сообщения на прикладной уровень
type OutputMessage struct {
	Sender   string    `json:"sender"`
	SendTime time.Time `json:"send_time"`
	Payload  string    `json:"payload"`           // Собранная полезная нагрузка
	Error    bool      `json:"error,omitempty"`   // Признак ошибки (опускается, если false)
	ErrorMsg string    `json:"error_msg,omitempty"` // Сообщение об ошибке (опускается, если пустое)
}

// Структура для хранения состояния сборки одного логического сообщения
type MessageReassemblyState struct {
	Segments                map[int]Segment // Хранит полученные сегменты по их номеру
	TotalSegmentsExpected   int             // Общее количество ожидаемых сегментов
	LastSegmentArrivalTime  time.Time       // Время поступления последнего сегмента для этого сообщения
	Sender					string
	SendTime				time.Time
}

// Коллекция незавершенных сообщений, ожидающих сегменты
// Ключ - уникальный идентификатор сообщения (например, Sender + SendTime)
// Значение - состояние сборки этого сообщения
var (
	inFlightMessages	map[string]*MessageReassemblyState
	inFlightMutex		sync.Mutex // Мьютекс для защиты доступа к map
)

// reassemblyGoroutine - Горутина для сборки сегментов из Kafka
func ReassemblyGoroutine() {
	// Инициализация Kafka Consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": KafkaAddr,
		"group.id":          "segment-reassembly-group",
		"auto.offset.reset": "earliest",
		// "debug": "all", // <<< ДОБАВЬТЕ ЭТУ СТРОКУ
		"enable.auto.commit": false,
		"session.timeout.ms": 10000, // Например, 10 секунд
		"heartbeat.interval.ms": 3000, // Например, 3 секунды
	})
	if err != nil {
		log.Fatalf("Не удалось создать Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Канал для событий потребителя
	eventsCh := consumer.Events()
	log.Printf("Kafka consumer подписан на топик: %s. Начинается цикл обработки...", KafkaTopic)

	err = consumer.SubscribeTopics([]string{KafkaTopic}, nil)
	if err != nil {
		log.Fatalf("Не удалось подписаться на топик %s: %v", KafkaTopic, err)
	}
	log.Printf("Kafka consumer подписан на топик: %s", KafkaTopic)

	// Инициализация коллекции незавершенных сообщений
	inFlightMessages = make(map[string]*MessageReassemblyState)

	// Настройка единого таймера для проверки всех сообщений (N = 1 секунда)
	ticker := time.NewTicker(BuildInterval)
	defer ticker.Stop()

	log.Println("Горутина сборки сегментов запущена.")
	for {
		select {
		case ev := <-eventsCh: // Обработка событий
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				log.Printf("Получены назначения разделов: %v", e)
				// Здесь потребитель готов начать чтение из этих разделов
			case kafka.RevokedPartitions:
				log.Printf("Отзыв разделов: %v", e)
				// Чтение из этих разделов прекращается
			case kafka.PartitionEOF:
				// Достигнут конец раздела
				log.Printf("Достигнут конец раздела %v", e)
			case kafka.Error:
				 if e.IsFatal() {
					log.Fatalf("Фатальная ошибка Kafka consumer: %v", e)
				 } else {
					 log.Printf("Нефатальная ошибка Kafka consumer: %v", e)
				 }
			default:
				// Другие события
				log.Printf("Получено событие Kafka: %v", e)
			}
		case <-ticker.C:
			// Тик таймера: Проверка таймаутов для всех незавершенных сообщений
			// log.Printf("Тик таймера: Проверка таймаутов, %v", inFlightMessages)
			now := time.Now()
			timedOutKeys := []string{}
			inFlightMutex.Lock()

			// Итерация по всем незавершенным сообщениям
			for key, state := range inFlightMessages {
				//? при тике провекре что должно быть раньше - удаление или проверка на количесво сегментов

				// Проверка условия сборки: если актуальное количество сегментов равно необходимому
				// количесву сегметов
				if len(state.Segments) == state.TotalSegmentsExpected {

					log.Printf("Сообщение по ключу '%s' полностью собрано.", key)

					// Обработка успешной сборки
					outputSuccessMessage := formatOutputMessage(state, true) // true - признак успеха
					fmt.Printf("Передача собранного сообщения (Успех): %s\n", string(outputSuccessMessage)) // Симуляция передачи на прикладной уровень

					// Помечаем ключ для удаления
					timedOutKeys = append(timedOutKeys, key)
				}

				// Проверка условия таймаута: если с момента поступления последнего сегмента
				// прошло больше времени, чем timeoutDuration
				if now.Sub(state.LastSegmentArrivalTime) > MaxInactivityCycles {

					log.Printf("Сообщение по ключу '%s' истек таймаут (ожидалось %d, получено %d)",
					key, state.TotalSegmentsExpected, len(state.Segments))

					// Обработка как ошибки доставки
					outputErrMessage := formatOutputMessage(state, false) // false - признак ошибки
					fmt.Printf("Передача собранного сообщения (Ошибка): %s\n", string(outputErrMessage)) // Симуляция передачи на прикладной уровень

					// Помечаем ключ для удаления
					timedOutKeys = append(timedOutKeys, key)
				}
			}

			// Удаление сообщений с истекшим таймаутом после итерации
			//* вынес удаление в отдельный цикл
			for _, key := range timedOutKeys {
				delete(inFlightMessages, key)
				log.Printf("Сообщение по ключу '%s' удалено из коллекции незавершенных.", key)
			}
			inFlightMutex.Unlock() // Снятие блокировки

		default:
			// Нет тика таймера, пробуем прочитать сообщение из Kafka
			// Чтение с небольшим таймаутом, чтобы не блокировать горутину вечно и позволить сработать таймеру
			msg, err := consumer.ReadMessage(100 * time.Millisecond)
			if err == nil {
				var segment Segment
				err = json.Unmarshal(msg.Value, &segment)
				if err != nil {
					log.Printf("Ошибка при десериализации сегмента: %v, содержимое: %s", err, string(msg.Value))
					// Пропускаем некорректное сообщение
					continue
				}

				log.Printf("Обработка сегмента %d/%d: Отправитель='%s', Время='%s'",
				segment.SegmentNumber, segment.TotalSegments, segment.Sender, segment.SendTime.Format(time.RFC3339))

				// Формируем уникальный ключ для сообщения (комбинация отправителя и времени отправки)
				messageKey := fmt.Sprintf("%s_%s", segment.Sender, segment.SendTime.Format(time.RFC3339Nano))

				inFlightMutex.Lock()

				// Найти или создать состояние сборки для данного сообщения
				state, exists := inFlightMessages[messageKey]
				if !exists {
					log.Printf("Начало сборки нового сообщения по ключу '%s'", messageKey)
					state = &MessageReassemblyState{
						Segments: make(map[int]Segment), // Инициализируем map для сегментов
						TotalSegmentsExpected: segment.TotalSegments,
						Sender: segment.Sender,
						SendTime: segment.SendTime,
					}
					inFlightMessages[messageKey] = state // Добавляем новое состояние в коллекцию
				} else {
					// Дополнительная проверка на согласованность
					if state.TotalSegmentsExpected != segment.TotalSegments || state.Sender != segment.Sender || !state.SendTime.Equal(segment.SendTime) {
						log.Printf("Предупреждение: Несоответствие метаданных сегмента для ключа '%s'. Пропускаем сегмент.", messageKey)
						inFlightMutex.Unlock() // Снимаем блокировку перед продолжением цикла
						continue
					}
				}

				// Добавляем сегмент, если он еще не был получен (обрабатываем дубликаты)
				if _, received := state.Segments[segment.SegmentNumber]; !received {
					state.Segments[segment.SegmentNumber] = segment
					state.LastSegmentArrivalTime = time.Now() // Обновляем время поступления последнего сегмента
					log.Printf("Добавлен сегмент %d/%d для сообщения '%s'. Получено %d из %d сегментов",
						segment.SegmentNumber, state.TotalSegmentsExpected, messageKey, len(state.Segments), segment.TotalSegments)
				} else {
					log.Printf("Получен дубликат сегмента %d для сообщения '%s'. Игнорируем.", segment.SegmentNumber, messageKey)
				}

				//* перенёс проверку на количество сегментов в прошлый case
				//* уменьшает частоту проверок и нагрузку
				//? утьчнить как праильнее по заданию

				// if len(state.Segments) == state.TotalSegmentsExpected {
				// 	log.Printf("Сообщение по ключу '%s' полностью собрано.", messageKey)

				// 	outputSuccessMessage := formatOutputMessage(state, true) // true - признак успеха
				// 	fmt.Printf("Передача собранного сообщения (Успех): %s\n", string(outputSuccessMessage)) // Симуляция передачи на прикладной уровень

				// 	delete(inFlightMessages, messageKey)
				// 	log.Printf("Сообщение по ключу '%s' удалено из коллекции незавершенных. Осталось: %d", messageKey, len(inFlightMessages))
				// }

				inFlightMutex.Unlock() // Снятие блокировки
			} else if err.(kafka.Error).IsFatal() {
				// Обработка фатальных ошибок Kafka
				//TODO Сделать логику попытки переподключения
				log.Fatalf("Фатальная ошибка Kafka consumer: %v", err)
			}
			// Нефатальные ошибки из ReadMessage (например, таймаут при отсутствии сообщений) игнорируются
		}
	}
}

// formatOutputMessage - Вспомогательная функция для форматирования финального JSON сообщения
func formatOutputMessage(state *MessageReassemblyState, success bool) []byte {
	output := OutputMessage{
		Sender: state.Sender,
		SendTime: state.SendTime,
	}

	// Проверка условия сборки: если собралось без ошиюки, то заполняем полезными данными, 
	// иначе заполняем пля с информацией об ошибки
	if success {
		// Собираем полезную нагрузку из сегментов по номеру
		payloadBuilder := strings.Builder{}
		//! Предполагаем, что сегменты имеют номера от 1 до TotalSegmentsExpected иначе цикл от 0
		for i := 1; i <= state.TotalSegmentsExpected; i++ {
			//* автоматичеси выставляетт сегменты в правильной последовательности, 
			//* даже если при обработке в очереди они попались раньше в истории брокера
			segment, ok := state.Segments[i]
			if ok {
				payloadBuilder.WriteString(segment.SegmentPayload)
			} else {
				// Эта ветка не должна выполняться при success=true, если количество сегментов совпадает с ожидаемым.
				// Но на всякий случай логируем.
				log.Printf("Ошибка при сборке полезной нагрузки: Отсутствует ожидаемый сегмент %d для сообщения '%s'",
					i, fmt.Sprintf("%s_%s", state.Sender, state.SendTime))
				// Здесь можно решить, что делать: собрать только имеющиеся части, или все равно пометить как ошибку.
				// В данном случае, при success=true, предполагается, что все сегменты 1..TotalSegmentsExpected присутствуют.
				// Если логика выше корректна, сюда мы не попадем при success=true.
			}
		}
		output.Payload = payloadBuilder.String()
		output.Error = false // Опускается при маршалинге
	} else {
		// Случай ошибки доставки (таймаут)
		output.Error = true
		output.ErrorMsg = fmt.Sprintf("Истек таймаут сообщения. Ожидалось %d сегментов, получено %d.",
			state.TotalSegmentsExpected, len(state.Segments))
		output.Payload = "" // Пустая полезная нагрузка при ошибке, как запрошено
	}

	// Маршалинг в JSON
	jsonOutput, err := json.Marshal(output)
	if err != nil {
		log.Printf("Ошибка при маршалинге финального сообщения: %v", err)
		// Возвращаем базовую структуру ошибки, если маршалинг не удался
		return []byte(fmt.Sprintf(`{"sender":"%s","send_time":"%s","error":true,"error_msg":"Внутренняя ошибка форматирования вывода"}`, state.Sender, state.SendTime))
	}

	return jsonOutput
}