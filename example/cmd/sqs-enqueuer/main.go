package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	kyu "github.com/maku693/jobqueue"
)

const (
	JobKindExample = "Example"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		slog.Error("error loading aws config", slog.Any("err", err))
		os.Exit(1)
	}
	client := sqs.NewFromConfig(cfg)

	queueUrl := os.Getenv("QUEUE_URL")
	if queueUrl == "" {
		slog.Error("QUEUE_URL env var is required")
		os.Exit(1)
	}

	e := &kyu.SQSEnqueuer{
		Client:   client,
		QueueUrl: queueUrl,
	}

	for i := 0; i < 10; i++ {
		kind := JobKindExample
		data, _ := json.Marshal(fmt.Sprintf("hello (%d)", i))
		slog.Info("enqueueing job", slog.String("kind", kind), slog.Any("data", data))
		id, err := e.Enqueue(
			context.Background(),
			kind,
			data,
			&kyu.EnqueueOptions{
				PerformAt: time.Now().Add(time.Duration(5+i) * time.Second),
			},
		)
		if err != nil {
			slog.Error("error enqueuing job", slog.Any("err", err))
			continue
		}
		slog.Info("job enqueued", slog.Any("id", id))
	}
}
