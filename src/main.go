package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/gorilla/mux"

	"securechat-transport/src/app"
)

func main() {
	log.Println("Запуск приложения...")

	// Контекст для graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Канал для сигналов ОС (например, Ctrl+C)
	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, os.Kill)

	var wg sync.WaitGroup // Для отслеживания завершения горутин

	// Горутина для обработки сигналов ОС
	wg.Add(1)
	go func() {
		defer wg.Done()
		sig := <-osSignals // Ожидание сигнала
		log.Printf("Получен сигнал '%s'. Инициирую штатное завершение...", sig)
		cancel() // Отменяем контекст
	}()

	// Запуск горутины для обработки Kafka
	wg.Add(1)
	go func() {
		defer wg.Done()
		app.ReassemblyGoroutine(ctx)
		log.Println("Горутина сборки сегментов завершила работу.")
	}()

	// Настройка маршрутов и HTTP сервера
	r := mux.NewRouter()
	r.HandleFunc("/send", app.HandleSend).Methods(http.MethodPost, http.MethodOptions)
	r.HandleFunc("/transfer", app.HandleTransfer).Methods(http.MethodPost, http.MethodOptions)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
		// Таймауты для предотвращения утечек соединений
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Запуск HTTP сервера в горутине
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Println("Запуск HTTP сервера на порту 8080...")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Ошибка при запуске сервера: %v", err)
		}
		log.Println("HTTP сервер завершил работу.")
	}()

	// Ожидаем отмены контекста (сигнал прерывания)
	<-ctx.Done()
	log.Println("Контекст отменен. Начинается штатное завершение...")

	// Штатное завершение HTTP сервера с таймаутом
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Ошибка штатного завершения сервера: %v", err)
	}
	log.Println("HTTP сервер штатно завершен.")

	// Ожидаем завершения всех горутин
	log.Println("Ожидание завершения всех горутин...")
	wg.Wait()

	log.Println("Приложение успешно завершено.")
}
