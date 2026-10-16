package config

import (
	"os"
)

type Config struct {
	Env      string
	HTTPAddr string
	DbUrl    string
}

func LoadConfig() Config {
	return Config{
		Env:      getenv("ENV", "development"),
		HTTPAddr: getenv("HTTP_ADDR", ":8000"),
		DbUrl:    getenv("DB_URL", "postgres://postgres:postgres@127.0.0.1:5433/webhooks?sslmode=disable"),
	}
}

func getenv(key, value string) string {

	if v := os.Getenv(key); v != "" {
		return v
	}
	return value
}
