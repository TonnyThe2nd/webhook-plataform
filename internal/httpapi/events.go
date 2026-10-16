package httpapi

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/TonnyThe2nd/webhook-platform/internal/db"
	"github.com/TonnyThe2nd/webhook-platform/internal/domain"
	"github.com/TonnyThe2nd/webhook-platform/internal/queue"
)

type Handler struct {
	store *db.Store
	queue *queue.SQS
}

func NewHandler(store *db.Store, q *queue.SQS) *Handler {
	return &Handler{store: store, queue: q}
}

type createEventRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type createEventResponse struct {
	EventID    string `json:"event_id"`
	Deliveries int    `json:"deliveries"`
	Created    bool   `json:"created"`
}

func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	tenantID := r.Header.Get("X-Tenant-ID")
	if tenantID == "" {
		http.Error(w, "X-Tenant-ID obrigatório", http.StatusBadRequest)
		return
	}

	var req createEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json inválido", http.StatusBadRequest)
		return
	}
	if req.Type == "" || len(req.Payload) == 0 {
		http.Error(w, "type e payload são obrigatórios", http.StatusBadRequest)
		return
	}

	idemKey := r.Header.Get("Idempotency-Key")

	event, created, err := h.store.InsertEvent(ctx, &domain.Event{
		TenantID:       tenantID,
		Type:           req.Type,
		Payload:        req.Payload,
		IdempotencyKey: idemKey,
	})
	if err != nil {
		log.Printf("erro criando evento: %v", err)
		http.Error(w, "erro criando evento", http.StatusInternalServerError)
		return
	}

	if !created {
		writeJSON(w, http.StatusOK, createEventResponse{
			EventID: event.ID,
			Created: false,
		})
		return
	}

	endpoints, err := h.store.EndpointsSubscribedTo(ctx, tenantID, req.Type)
	if err != nil {
		log.Printf("erro buscando endpoints: %v", err)
		http.Error(w, "erro buscando endpoints", http.StatusInternalServerError)
		return
	}

	count := 0
	for _, ep := range endpoints {
		d, err := h.store.InsertDelivery(ctx, event.ID, ep.ID, tenantID)
		if err != nil {
			continue
		}
		_ = h.queue.Enqueue(ctx, queue.DeliveryMessage{DeliveryID: d.ID, Attempt: 0}, 0)
		count++
	}

	writeJSON(w, http.StatusAccepted, createEventResponse{
		EventID:    event.ID,
		Deliveries: count,
		Created:    true,
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
