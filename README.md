# securechat-transport

## Описание проекта
Этот проект демонстрирует использование Apache Kafka в связке с Go. В проекте реализованы:
- Producer (отправитель сообщений в Kafka)
- Consumer (читатель сообщений из Kafka)
- Сегментация сообщений
- Сборка сегментов с проверкой потерь
- Отправка сообщений на WebSocket сервер прикладного уровня

Kafka разворачивается в Docker, а взаимодействие с ней осуществляется через библиотеку [sarama](https://github.com/IBM/sarama).

## Структура проекта
- docker-compose.yml — настройки для развертывания Kafka и Zookeeper
- broker.go — файл с реализацией producer-а и consumer-а
- segment.go — логика сегментации и сборки сообщений
- assembly.go — логика сборки сообщений
- http_send.go, http_transfer.go — обработчики HTTP-запросов
- ws.go — отправка сообщений на WebSocket сервер

## Установка и запуск

### 1. Инициализация окружения

```sh
$ make init
```
Запускает Docker Compose и устанавливает зависимости Go.

### 2. Создание топика в Kafka
Выполните команды в контейнере Kafka:

```sh
$ sudo docker exec -it kafka sh
> /bin/kafka-topics --create --topic segments --bootstrap-server localhost:9092
```

### 3. Отправка тестового сообщения

```sh
> /bin/kafka-console-producer --topic segments --bootstrap-server localhost:9092
```
Введите JSON-сообщение:

```json
> {"segment_number": 1, "total_segments": 1, "username": "test_user", "send_time": "2024-05-21T02:34:48Z", "payload": "Hello, world!"}
```

### 4. Запуск программы

```sh
$ make run
```
Сначала собирает build, затем запускает программу.

## API Эндпоинты
| Метод | URL | Описание |
|--------|-----|-------------|
| POST | /transfer | Отправка сегмента в Kafka от канального уровня|
| POST | /send | Разбиение сообщения на сегменты и их отправка |

Используйте Postman или curl для отправки сегмента:

```sh
curl -X POST http://localhost:8080/transfer \
     -H "Content-Type: application/json" \
     -d '{"segment_number": 1, "total_segments": 1, "username": "test_user", "send_time": "2024-05-21T02:34:48Z", "payload": "Hello, world!"}'
```

Используйте следующий запрос для отправки сообщения:

```sh
curl -X POST http://localhost:8080/send \
     -H "Content-Type: application/json" \
     -d '{"username": "test_user", "data": "This is a test message", "send_time": "2024-05-21T02:34:48Z"}'
```

## Отправка сообщений на WebSocket сервер
После того, как сообщение собрано из сегментов, оно отправляется на сервер WebSocket. Ваш сервер WebSocket должен быть готов принимать HTTP-запросы с JSON-данными по следующему адресу:

**URL:** `http://192.168.123.140:3000/receive`

Пример отправляемого JSON:

```json
{
  "username": "test_user",
  "message": "This is a complete message",
  "timestamp": "2024-05-21T02:34:48Z"
}
```

Ваш WebSocket сервер должен обработать этот запрос и передать сообщение конечному получателю, например, через открытое WebSocket соединение с клиентом.

## Полезные ссылки
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Sarama - Go Kafka Library](https://github.com/IBM/sarama)