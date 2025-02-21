package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"securechat-transport/src/app"

	"github.com/gorilla/mux"
)

//{"segment_number": 1, "total_segments": 1, "username": "test_user", "send_time": "2024-05-21T02:34:48Z", "payload": "Hello, world!"}

func main() {
	// запуск consumer-а
	go func() {
		if err := app.ReadFromKafka(); err != nil {
			fmt.Println(err)
		}
	}()

	go func() {
		ticker := time.NewTicker(app.KafkaReadPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				app.ScanStorage(app.SendReceiveRequest)
			}
		}
	}()

	// создание роутера
	r := mux.NewRouter()
	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Not Found", http.StatusNotFound)
	})
	http.Handle("/", r)
	r.HandleFunc("/transfer", app.HandleTransfer).Methods(http.MethodPost, http.MethodOptions)
	r.HandleFunc("/send", app.HandleSend).Methods(http.MethodPost, http.MethodOptions)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	// запуск http сервера
	srv := http.Server{
		Handler:           r,
		Addr:              ":8080",
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			fmt.Println("Server stopped")
		}
	}()
	fmt.Println("Server started")

	// graceful shutdown
	sig := <-signalCh
	fmt.Printf("Received signal: %v\n", sig)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		fmt.Printf("Server shutdown failed: %v\n", err)
	}
}
