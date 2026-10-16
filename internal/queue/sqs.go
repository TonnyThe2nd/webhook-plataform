package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQS struct {
	client *sqs.Client
	url    string
}

func NewSQS(client *sqs.Client, url string) *SQS {
	return &SQS{client: client, url: url}
}

func (q *SQS) Enqueue(ctx context.Context, msg DeliveryMessage, delay time.Duration) error {
	body, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("serializando mensagem: %w", err)
	}

	delaySec := int32(delay.Seconds())
	if delaySec < 0 {
		delaySec = 0
	}
	if delaySec > 900 {
		delaySec = 900
	}

	_, err = q.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:     aws.String(q.url),
		MessageBody:  aws.String(body),
		DelaySeconds: delaySec,
	})
	if err != nil {
		return fmt.Errorf("enviando para sqs: %w", err)
	}
	return nil
}

func (q *SQS) Receive(ctx context.Context, max int, wait time.Duration) ([]sqstypes.Message, error) {
	out, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.url),
		MaxNumberOfMessages: int32(max),
		WaitTimeSeconds:     int32(wait.Seconds()),
		VisibilityTimeout:   30,
	})
	if err != nil {
		return nil, fmt.Errorf("recebendo da sqs: %w", err)
	}
	return out.Messages, nil
}

func (q *SQS) Delete(ctx context.Context, receiptHandle string) error {
	_, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.url),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("deletando mensagem: %w", err)
	}
	return nil
}
