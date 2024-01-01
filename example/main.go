package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/maku693/kyu"
	"github.com/maku693/kyu/awskyu"
)

func main() {
	client := sqs.New(sqs.Options{}, nil)
	queueUrl := ""
	var queue kyu.Queue = awskyu.NewSQSQueue(client, queueUrl)

	go func() {
		for {
			if err := queue.SendMessage(context.Background(), "message"); err != nil {
				slog.Error("failed to send message", slog.Any("err", err))
			}
			time.Sleep(1 * time.Second)
		}
	}()

	for {
		msg, err := queue.ReceiveMessage(context.Background())
		if err != nil {
			slog.Error("failed to receive message", slog.Any("err", err))
		}
		slog.Info("received message", slog.Any("body", msg.Body()))
	}
}
