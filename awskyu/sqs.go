package awskyu

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/maku693/kyu"
)

type SQSQueue struct {
	client   *sqs.Client
	queueUrl string
}

func NewSQSQueue(client *sqs.Client, queueUrl string) *SQSQueue {
	return &SQSQueue{
		client:   client,
		queueUrl: queueUrl,
	}
}

func (q *SQSQueue) SendMessage(ctx context.Context, body kyu.MessageBody) error {
	messageBody, ok := body.(string)
	if !ok {
		return fmt.Errorf("body should be string; %t given", body)
	}
	_, err := q.client.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: aws.String(messageBody),
		QueueUrl:    aws.String(q.queueUrl),
	})
	if err != nil {
		return err
	}
	return nil
}

func (q *SQSQueue) ReceiveMessage(ctx context.Context) (kyu.Message, error) {
	msg, err := q.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: aws.String(q.queueUrl),
	})
	if err != nil {
		return nil, err
	}
	return &SQSMessage{
		id:   *msg.Messages[0].ReceiptHandle,
		body: *msg.Messages[0].Body,
	}, nil
}

func (q *SQSQueue) DeleteMessage(ctx context.Context, msgid kyu.MessageID) error {
	receiptHandle, ok := msgid.(string)
	if !ok {
		return fmt.Errorf("msgid should be string; %t given", msgid)
	}
	_, err := q.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.queueUrl),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return err
	}
	return nil
}

var _ kyu.Queue = new(SQSQueue)

type SQSMessage struct {
	id   string
	body string
}

func (msg *SQSMessage) ID() kyu.MessageID {
	return msg.id
}

func (msg *SQSMessage) Body() kyu.MessageBody {
	return msg.body
}

var _ kyu.Message = new(SQSMessage)
