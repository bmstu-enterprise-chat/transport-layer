package app

import (
	"encoding/json"
	"io"
	"net/http"
)

func HandleTransfer(w http.ResponseWriter, r *http.Request) {
	// читаем тело запроса - сегмент
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// парсим сегмент в структуру
	segment := Segment{}
	if err = json.Unmarshal(body, &segment); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// пишем сегмент в Kafka
	if err = WriteToKafka(segment); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
