package dispatcher_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TonnyThe2nd/webhook-platform/internal/db"
	"github.com/TonnyThe2nd/webhook-platform/internal/dispatcher"
	"github.com/TonnyThe2nd/webhook-platform/internal/domain"
	"github.com/TonnyThe2nd/webhook-platform/internal/httpapi"
	"github.com/TonnyThe2nd/webhook-platform/internal/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type integrationQueue struct{ *queue.SQS }

func (q integrationQueue) Receive(ctx context.Context, max int, wait time.Duration) ([]dispatcher.Message, error) {
	msgs, err := q.SQS.Receive(ctx, max, wait)
	if err != nil {
		return nil, err
	}
	out := make([]dispatcher.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, dispatcher.Message{Body: aws.ToString(m.Body), ReceiptHandle: aws.ToString(m.ReceiptHandle)})
	}
	return out, nil
}
func TestWorkerIntegration(t *testing.T) {
	if os.Getenv("WEBHOOK_INTEGRATION_TEST") != "1" {
		t.Skip("set WEBHOOK_INTEGRATION_TEST=1 to test against local PostgreSQL and LocalStack")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, err := db.ConnectDB(ctx, "postgres://postgres:postgres@127.0.0.1:5433/webhooks?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := db.NewStore(pool)
	client := sqs.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""),
	}, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://127.0.0.1:4566") })

	for _, tc := range []struct {
		name         string
		failures     int32
		wantStatus   domain.DeliveryStatus
		wantAttempts int
	}{
		{"success", 0, domain.DeliveryDelivered, 1},
		{"retry_then_success", 1, domain.DeliveryDelivered, 2},
		{"exhausted_retries", 100, domain.DeliveryFailed, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			createdQueue, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(fmt.Sprintf("worker-test-%d", time.Now().UnixNano())),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				if _, err := client.DeleteQueue(cleanup, &sqs.DeleteQueueInput{QueueUrl: createdQueue.QueueUrl}); err != nil {
					t.Errorf("cleaning test queue: %v", err)
				}
			}()
			q := queue.NewSQS(client, aws.ToString(createdQueue.QueueUrl))
			router := httpapi.NewRouter(store, q)
			var calls atomic.Int32
			receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) <= tc.failures {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer receiver.Close()
			var tenantID string
			if err := pool.QueryRow(ctx, `INSERT INTO tenants (name) VALUES ('worker integration test') RETURNING id`).Scan(&tenantID); err != nil {
				t.Fatal(err)
			}
			defer func() {
				cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				if _, err := pool.Exec(cleanup, `DELETE FROM tenants WHERE id = $1`, tenantID); err != nil {
					t.Errorf("cleaning test tenant: %v", err)
				}
			}()
			if _, err := pool.Exec(ctx, `INSERT INTO endpoints (tenant_id, url, secret) VALUES ($1, $2, 'integration-secret')`, tenantID, receiver.URL); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(`{"type":"worker.test","payload":{"test":true}}`))
			req = req.WithContext(ctx)
			req.Header.Set("X-Tenant-ID", tenantID)
			req.Header.Set("Content-Type", "application/json")
			resp := httptest.NewRecorder()
			router.ServeHTTP(resp, req)
			if resp.Code != http.StatusAccepted {
				t.Fatalf("API status=%d body=%s", resp.Code, resp.Body.String())
			}
			var event struct {
				EventID    string `json:"event_id"`
				Deliveries int    `json:"deliveries"`
			}
			if err := json.Unmarshal(resp.Body.Bytes(), &event); err != nil {
				t.Fatal(err)
			}
			if event.Deliveries != 1 {
				t.Fatalf("expected one delivery, got %d", event.Deliveries)
			}
			var deliveryID string
			if err := pool.QueryRow(ctx, `SELECT id FROM deliveries WHERE event_id = $1`, event.EventID).Scan(&deliveryID); err != nil {
				t.Fatal(err)
			}
			before, err := store.GetDelivery(ctx, deliveryID)
			if err != nil {
				t.Fatal(err)
			}
			if before.Status != domain.DeliveryPending || before.Attempts != 0 || calls.Load() != 0 {
				t.Fatal("delivery should remain pending before starting worker")
			}
			t.Log("worker stopped: delivery pending, attempts=0, destination calls=0")
			workerCtx, stopWorker := context.WithCancel(ctx)
			done := make(chan error, 1)
			worker := dispatcher.New(store, integrationQueue{q}, dispatcher.Config{
				Concurrency: 1, MaxAttempts: 3, BackoffBase: time.Second,
				BackoffCap: time.Second, HTTPTimeout: time.Second,
			}).WithLogger(t.Logf)
			go func() { done <- worker.Run(workerCtx) }()
			defer func() {
				stopWorker()
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("worker: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Error("worker did not stop")
				}
			}()
			deadline := time.NewTimer(20 * time.Second)
			defer deadline.Stop()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-deadline.C:
					t.Fatal("timed out waiting for worker")
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-ticker.C:
					d, err := store.GetDelivery(ctx, deliveryID)
					if err != nil {
						t.Fatal(err)
					}
					if d.Status != tc.wantStatus {
						continue
					}
					var attempts int
					if err := pool.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE delivery_id = $1`, deliveryID).Scan(&attempts); err != nil {
						t.Fatal(err)
					}
					if d.Attempts != tc.wantAttempts || attempts != tc.wantAttempts || int(calls.Load()) != tc.wantAttempts {
						t.Fatalf("attempts=%d records=%d HTTP calls=%d, expected=%d", d.Attempts, attempts, calls.Load(), tc.wantAttempts)
					}
					t.Logf("worker running: status=%s, attempts=%d, HTTP calls=%d", d.Status, d.Attempts, calls.Load())
					return
				}
			}
		})
	}
}
