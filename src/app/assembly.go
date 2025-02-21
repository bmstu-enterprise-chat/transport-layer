package app

import (
	"fmt"
	"sync"
	"time"
)

type Message struct {
	Received int
	Total    int
	Last     time.Time
	Username string
	Segments []string
}

type Storage map[time.Time]Message

var storage = Storage{}

func addMessage(segment Segment) {
	storage[segment.SendTime] = Message{
		Received: 0,
		Total:    segment.TotalSegments,
		Last:     time.Now().UTC(),
		Username: segment.Username,
		Segments: make([]string, segment.TotalSegments), // заранее выделяем память, это важно!
	}
}

func AddSegment(segment Segment) {
	// используем мьютекс, чтобы избежать конкуретного доступа к хранилищу
	mu := &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()

	// если это первый сегмент сообщения, создаем пустое сообщение
	sendTime := segment.SendTime
	_, found := storage[sendTime]
	if !found {
		addMessage(segment)
	}

	// добавляем в сообщение информацию о сегменте
	message, _ := storage[sendTime]
	message.Received++
	message.Last = time.Now().UTC()
	message.Segments[segment.SegmentNumber-1] = segment.SegmentPayload // сохраняем правильный порядок сегментов
	storage[sendTime] = message
}

func getMessageText(sendTime time.Time) string {
	result := ""
	message, _ := storage[sendTime]
	for _, segment := range message.Segments {
		result += segment
	}
	return result
}

const (
	SegmentLostError = "lost"
	KafkaReadPeriod  = 2 * time.Second
)

// структура тела запроса на прикладной уровень
type ReceiveRequest struct {
	Username string    `json:"username"`
	Text     string    `json:"data"`
	SendTime time.Time `json:"send_time"`
	Error    string    `json:"error"`
}
type sendFunc func(body ReceiveRequest)

func ScanStorage(sender sendFunc) {
	mu := &sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()

	payload := ReceiveRequest{}
	for sendTime, message := range storage {
		if message.Received == message.Total { // если пришли все сегменты
			payload = ReceiveRequest{
				Username: message.Username,
				Text:     getMessageText(sendTime), // склейка сообщения
				SendTime: sendTime,
				Error:    "",
			}
			fmt.Printf("sent message: %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		} else if time.Since(message.Last) > KafkaReadPeriod+time.Second { // если канальный уровень потерял сегмент
			payload = ReceiveRequest{
				Username: message.Username,
				Text:     "",
				SendTime: sendTime,
				Error:    SegmentLostError, // ошибка
			}
			fmt.Printf("sent error: %+v\n", payload)
			go sender(payload)        // запускаем горутину с отправкой на прикладной уровень, не будем дожидаться результата ее выполнения
			delete(storage, sendTime) // не забываем удалять
		}
	}
}
