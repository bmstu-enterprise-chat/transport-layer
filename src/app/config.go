package app

import "time"

// Константы конфигурации
const (
	SegmentSize			= 140				// Размер сегмента в байтах
	BuildInterval		= 1 * time.Second	// Интервал проверки для сборки
	MaxInactivityCycles	= 3 * time.Second	// Максимальный интервал без новых сегментов для ошибки
	urlChannelLevel		= "http://localhost:8081/code"
	urlApplLevel		= "http://localhost:3000/receive"
)

//! Перенести в env
// Данные kafka
const (
	KafkaAddr  = "localhost:29092"
	KafkaTopic = "segments"
)