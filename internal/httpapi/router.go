package httpapi

import (
	"net/http"

	"github.com/TonnyThe2nd/webhook-platform/internal/db"
	"github.com/TonnyThe2nd/webhook-platform/internal/queue"
)

func NewRouter(store *db.Store, q *queue.SQS) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)

	h := NewHandler(store, q)
	mux.HandleFunc("POST /v1/events", h.CreateEvent)

	return mux
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
