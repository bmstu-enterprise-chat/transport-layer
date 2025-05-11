package main

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"

	"securechat-transport/src/app"
)


func main() {
	log.Println("Запуск горутины сборки сегментов...")

	// Запускаем горутину для обработки Kafka
	go app.ReassemblyGoroutine()

    // Сервер для обработки /send на порту 8080
    r := mux.NewRouter()
    r.HandleFunc("/send", app.HandleSend).Methods(http.MethodPost, http.MethodOptions)
	r.HandleFunc("/transfer", app.HandleTransfer).Methods(http.MethodPost, http.MethodOptions)

    // Запуск основного сервера на порту 8080
    if err := http.ListenAndServe(":8080", r); err != nil {
        log.Fatalf("Ошибка при запуске сервера: %v", err)
    }
}
