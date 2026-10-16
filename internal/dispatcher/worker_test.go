package dispatcher

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/TonnyThe2nd/webhook-platform/internal/domain"
	"github.com/TonnyThe2nd/webhook-platform/internal/queue"
)

type testStore struct {
	delivery                                 *domain.Delivery
	fail                                     string
	recorded, delivered, failed, rescheduled bool
}

func (s *testStore) GetDelivery(context.Context, string) (*domain.Delivery, error) {
	if s.fail == "get" {
		return nil, errors.New("database unavailable")
	}
	return s.delivery, nil
}
func (s *testStore) RecordAttempt(context.Context, string, int, int, int, string) error {
	s.recorded = true
	if s.fail == "record" {
		return errors.New("database unavailable")
	}
	return nil
}
func (s *testStore) MarkDelivered(context.Context, string, int, int) error {
	s.delivered = true
	if s.fail == "delivered" {
		return errors.New("database unavailable")
	}
	return nil
}
func (s *testStore) MarkFailed(context.Context, string, int, string) error {
	s.failed = true
	if s.fail == "failed" {
		return errors.New("database unavailable")
	}
	return nil
}
func (s *testStore) Reschedule(context.Context, string, int, time.Time, int, string) error {
	s.rescheduled = true
	if s.fail == "reschedule" {
		return errors.New("database unavailable")
	}
	return nil
}

type testQueue struct {
	deleted, enqueued bool
	fail              bool
	message           queue.DeliveryMessage
	delay             time.Duration
}

func (q *testQueue) Receive(context.Context, int, time.Duration) ([]Message, error) { return nil, nil }
func (q *testQueue) Delete(context.Context, string) error                           { q.deleted = true; return nil }
func (q *testQueue) Enqueue(_ context.Context, msg queue.DeliveryMessage, delay time.Duration) error {
	q.enqueued, q.message, q.delay = true, msg, delay
	if q.fail {
		return errors.New("queue unavailable")
	}
	return nil
}

func TestProcessMessage(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		status                           int
		fail                             string
		queueFail                        bool
		attempt                          int
		delete, delivered, failed, retry bool
	}{
		{name: "success", status: 204, delete: true, delivered: true},
		{name: "retry", status: 503, delete: true, retry: true},
		{name: "last attempt", status: 503, attempt: 2, delete: true, failed: true},
		{name: "database unavailable", status: 204, fail: "get"},
		{name: "attempt not recorded", status: 204, fail: "record"},
		{name: "success not persisted", status: 204, fail: "delivered", delivered: true},
		{name: "failure not persisted", status: 503, attempt: 2, fail: "failed", failed: true},
		{name: "reschedule not persisted", status: 503, fail: "reschedule"},
		{name: "retry not enqueued", status: 503, queueFail: true, retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(`{"order_id":"123"}`)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
				}
				if r.Method != http.MethodPost || string(body) != string(payload) {
					t.Error("unexpected request")
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Error("missing JSON content type")
				}
				mac := hmac.New(sha256.New, []byte("secret"))
				mac.Write([]byte(r.Header.Get("X-Webhook-Timestamp") + "."))
				mac.Write(body)
				if r.Header.Get("X-Webhook-Signature") != hex.EncodeToString(mac.Sum(nil)) {
					t.Error("invalid signature")
				}
				if r.Header.Get("X-Webhook-Id") != "delivery-1" || r.Header.Get("X-Webhook-Event") != "order.paid" {
					t.Error("missing webhook identity")
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			s := &testStore{fail: tc.fail, delivery: &domain.Delivery{
				ID: "delivery-1", Status: domain.DeliveryPending, URL: server.URL,
				Secret: "secret", EventType: "order.paid", Payload: payload,
			}}
			q := &testQueue{fail: tc.queueFail}
			w := New(s, q, Config{MaxAttempts: 3, BackoffBase: time.Second, BackoffCap: time.Minute})
			body, err := (queue.DeliveryMessage{DeliveryID: "delivery-1", Attempt: tc.attempt}).Marshal()
			if err != nil {
				t.Fatal(err)
			}
			w.processMessage(context.Background(), Message{Body: body, ReceiptHandle: "receipt"})
			if q.deleted != tc.delete || s.delivered != tc.delivered || s.failed != tc.failed || q.enqueued != tc.retry {
				t.Fatalf("deleted=%v delivered=%v failed=%v retry=%v", q.deleted, s.delivered, s.failed, q.enqueued)
			}
			if tc.retry && (q.message.Attempt != tc.attempt+1 || q.delay != time.Second) {
				t.Error("incorrect retry")
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{
		{1, 5 * time.Second}, {2, 10 * time.Second}, {3, 20 * time.Second},
		{4, 30 * time.Second}, {1000000, 30 * time.Second},
	} {
		if got := Backoff(tc.attempt, 5*time.Second, 30*time.Second); got != tc.want {
			t.Errorf("attempt %d: got %v, want %v", tc.attempt, got, tc.want)
		}
	}
}
