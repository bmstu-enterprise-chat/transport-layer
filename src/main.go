package main

import (
	"net/http"
	"log"
	"fmt"

	"securechat-transport/src/app"

	"github.com/gorilla/mux"
)

func HandleCode(w http.ResponseWriter, r *http.Request) {
    log.Println("Получен запрос на /code")
    w.WriteHeader(http.StatusOK)
    fmt.Fprintln(w, "i am ok")
}

func main() {
    // Сервер для обработки /send на порту 8080
    r := mux.NewRouter()
    r.HandleFunc("/send", app.HandleSend).Methods(http.MethodPost, http.MethodOptions)
	r.HandleFunc("/transfer", app.HandleTransfer).Methods(http.MethodPost, http.MethodOptions)

    // Сервер для обработки /code на порту 8081
    rHealth := mux.NewRouter()
    rHealth.HandleFunc("/code", HandleCode).Methods(http.MethodPost)
    go func() {
        if err := http.ListenAndServe(":8081", rHealth); err != nil {
            log.Fatalf("Ошибка при запуске сервера на 8081: %v", err)
        }
    }()

    // Запуск основного сервера на порту 8080
    if err := http.ListenAndServe(":8080", r); err != nil {
        log.Fatalf("Ошибка при запуске сервера: %v", err)
    }
}
