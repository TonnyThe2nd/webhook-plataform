package dispatcher

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/TonnyThe2nd/webhook-platform/internal/domain"
	"github.com/TonnyThe2nd/webhook-platform/internal/queue"
	"github.com/TonnyThe2nd/webhook-platform/internal/signer"
	"github.com/TonnyThe2nd/webhook-platform/pkg/httpx"
)

type Store interface {
	GetDelivery(ctx context.Context, id string) (*domain.Delivery, error)
	MarkDelivered(ctx context.Context, id string, statusCode int, latencyMs int) error
	MarkFailed(ctx context.Context, id string, statusCode int, errMsg string) error
	Reschedule(ctx context.Context, id string, attempt int, next time.Time, statusCode int, errMsg string) error
	RecordAttempt(ctx context.Context, deliveryID string, attemptNo, statusCode, latencyMs int, errMsg string) error
}

type Queue interface {
	Receive(ctx context.Context, max int, wait time.Duration) ([]Message, error)
	Delete(ctx context.Context, receiptHandle string) error
	Enqueue(ctx context.Context, msg queue.DeliveryMessage, delay time.Duration) error
}

type Message struct {
	Body          string
	ReceiptHandle string
}

type Worker struct {
	store       Store
	queue       Queue
	httpClient  *http.Client
	concurrency int
	maxAttempts int
	backoffBase time.Duration
	backoffCap  time.Duration
	logf        func(format string, args ...any)
}

type Config struct {
	Concurrency int
	MaxAttempts int
	BackoffBase time.Duration
	BackoffCap  time.Duration
	HTTPTimeout time.Duration
}

func New(store Store, q Queue, cfg Config) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 5 * time.Second
	}
	if cfg.BackoffCap <= 0 {
		cfg.BackoffCap = 5 * time.Minute
	}
	return &Worker{
		store:       store,
		queue:       q,
		httpClient:  httpx.NewClient(cfg.HTTPTimeout),
		concurrency: cfg.Concurrency,
		maxAttempts: cfg.MaxAttempts,
		backoffBase: cfg.BackoffBase,
		backoffCap:  cfg.BackoffCap,
		logf:        func(format string, args ...any) {},
	}
}

func (w *Worker) WithLogger(logf func(format string, args ...any)) *Worker {
	if logf != nil {
		w.logf = logf
	}
	return w
}

func (w *Worker) Run(ctx context.Context) error {
	sem := make(chan struct{}, w.concurrency)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			w.logf("worker parando, aguardando goroutines em andamento")
			wg.Wait()
			return nil
		default:
		}

		msgs, err := w.queue.Receive(ctx, 10, 20*time.Second)
		if err != nil {
			w.logf("erro recebendo da fila: %v", err)
			select {
			case <-ctx.Done():
				wg.Wait()
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}

		for _, m := range msgs {
			m := m
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				wg.Wait()
				return nil
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				w.processMessage(ctx, m)
			}()
		}
	}
}

func (w *Worker) processMessage(ctx context.Context, m Message) {
	msg, err := queue.UnmarshalDelivery(m.Body)
	if err != nil {
		w.logf("payload inválido na fila: %v", err)
		_ = w.queue.Delete(ctx, m.ReceiptHandle)
		return
	}

	d, err := w.store.GetDelivery(ctx, msg.DeliveryID)
	if err != nil {
		w.logf("delivery não encontrada: %s: %v", msg.DeliveryID, err)
		return
	}

	if d.Status == domain.DeliveryDelivered || d.Status == domain.DeliveryFailed {
		w.logf("delivery já entregue, ignorando: %s", d.ID)
		_ = w.queue.Delete(ctx, m.ReceiptHandle)
		return
	}

	attempt := msg.Attempt + 1
	timestamp := time.Now().Unix()
	signature := signer.Sign(d.Secret, timestamp, d.Payload)

	headers := map[string]string{
		"X-Webhook-Id":        d.ID,
		"X-Webhook-Event":     d.EventType,
		"X-Webhook-Timestamp": strconv.FormatInt(timestamp, 10),
		"X-Webhook-Signature": signature,
		"X-Webhook-Attempt":   strconv.Itoa(attempt),
	}

	statusCode, latency, err := httpx.Post(ctx, w.httpClient, d.URL, headers, d.Payload)
	latencyMs := int(latency.Milliseconds())
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}

	if err := w.store.RecordAttempt(ctx, d.ID, attempt, statusCode, latencyMs, errMsg); err != nil {
		w.logf("erro registrando tentativa de %s: %v", d.ID, err)
		return
	}

	success := err == nil && statusCode >= 200 && statusCode < 300
	if success {
		w.logf("delivery %s entregue em %dms (HTTP %d)", d.ID, latencyMs, statusCode)
		if err := w.store.MarkDelivered(ctx, d.ID, statusCode, latencyMs); err != nil {
			w.logf("erro marcando entrega %s: %v", d.ID, err)
			return
		}
		_ = w.queue.Delete(ctx, m.ReceiptHandle)
		return
	}

	if attempt >= w.maxAttempts {
		w.logf("delivery %s falhou após %d tentativas: HTTP %d %s", d.ID, attempt, statusCode, errMsg)
		if err := w.store.MarkFailed(ctx, d.ID, statusCode, errMsg); err != nil {
			w.logf("erro marcando falha de %s: %v", d.ID, err)
			return
		}
		_ = w.queue.Delete(ctx, m.ReceiptHandle)
		return
	}

	delay := Backoff(attempt, w.backoffBase, w.backoffCap)
	if delay > 15*time.Minute {
		delay = 15 * time.Minute
	}
	next := time.Now().Add(delay)
	w.logf("delivery %s falhou (HTTP %d), retry em %s", d.ID, statusCode, delay)

	if err := w.store.Reschedule(ctx, d.ID, attempt, next, statusCode, errMsg); err != nil {
		w.logf("erro reagendando %s: %v", d.ID, err)
		return
	}
	if err := w.queue.Enqueue(ctx, queue.DeliveryMessage{DeliveryID: d.ID, Attempt: attempt}, delay); err != nil {
		w.logf("erro enfileirando retry de %s: %v", d.ID, err)
		return
	}
	_ = w.queue.Delete(ctx, m.ReceiptHandle)
}
