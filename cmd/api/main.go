package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/TonnyThe2nd/webhook-platform/internal/config"
	"github.com/TonnyThe2nd/webhook-platform/internal/db"
	"github.com/TonnyThe2nd/webhook-platform/internal/httpapi"
	"github.com/TonnyThe2nd/webhook-platform/internal/queue"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
	cfg := config.LoadConfig()
	ctx := context.Background()

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

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: httpapi.NewRouter(store, q),
	}

	log.Printf("api ouvindo em %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
