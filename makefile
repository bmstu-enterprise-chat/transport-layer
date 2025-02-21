.PHONY: init build run debug clean

PROCESS_NAME = main
BUILD_DIR = bin
SRC_DIR = src
EXECUTABLE = $(BUILD_DIR)/$(PROCESS_NAME)

# Проверка существования директорий
$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

# Инициализация окружения (Docker + зависимости)
init:
	@echo "Запуск Docker Compose..."
	@sudo docker compose up -d || (echo "Ошибка запуска Docker Compose" && exit 1)
	@echo "Установка зависимостей Go..."
	@go mod tidy
	@echo "Инициализация завершена."

# Сборка Go-программы
build: $(BUILD_DIR)
	@echo "Сборка Go-программы..."
	@go build -v -o $(EXECUTABLE) $(SRC_DIR)/main.go || (echo "Ошибка сборки" && exit 1)
	@echo "Сборка завершена. Программа сохранена в $(EXECUTABLE)."

# Запуск программы после сборки
run: build
	@echo "Запуск Go-программы..."
	@$(EXECUTABLE)

# Отладочный запуск (без предварительной сборки)
debug:
	@echo "Запуск Go-программы (debug mode)..."
	@go run $(SRC_DIR)/main.go || (echo "Ошибка выполнения" && exit 1)

# Очистка скомпилированных файлов
clean:
	@echo "Очистка проекта..."
	@rm -rf $(BUILD_DIR)
	@echo "Очистка завершена."
