package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TonnyThe2nd/webhook-platform/internal/config"
	"github.com/TonnyThe2nd/webhook-platform/internal/db"
	"github.com/TonnyThe2nd/webhook-platform/internal/dispatcher"
	"github.com/TonnyThe2nd/webhook-platform/internal/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type adapterSQS struct {
	q *queue.SQS
}

func (a *adapterSQS) Receive(ctx context.Context, max int, wait time.Duration) ([]dispatcher.Message, error) {
	msgs, err := a.q.Receive(ctx, max, wait)
	if err != nil {
		return nil, err
	}
	out := make([]dispatcher.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, dispatcher.Message{
			Body:          aws.ToString(m.Body),
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
		})
	}
	return out, nil
}

func (a *adapterSQS) Delete(ctx context.Context, receiptHandle string) error {
	return a.q.Delete(ctx, receiptHandle)
}

func (a *adapterSQS) Enqueue(ctx context.Context, msg queue.DeliveryMessage, delay time.Duration) error {
	return a.q.Enqueue(ctx, msg, delay)
}

func main() {
	cfg := config.LoadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.ConnectDB(ctx, cfg.DbUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	store := db.NewStore(pool)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(os.Getenv("AWS_REGION")),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		log.Fatal(err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(os.Getenv("AWS_ENDPOINT_URL"))
	})
	q := queue.NewSQS(sqsClient, os.Getenv("QUEUE_URL"))

	worker := dispatcher.New(store, &adapterSQS{q: q}, dispatcher.Config{
		Concurrency: 20,
		MaxAttempts: 5,
		BackoffBase: 5 * time.Second,
		BackoffCap:  5 * time.Minute,
		HTTPTimeout: 10 * time.Second,
	}).WithLogger(log.Printf)

	log.Println("worker iniciado")
	if err := worker.Run(ctx); err != nil {
		log.Fatal(err)
	}
	log.Println("worker encerrado")
}
