package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/maku693/jobqueue"
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

	d := &jobqueue.SQSDequeuer{
		Client:              client,
		QueueUrl:            "",
		MaxNumberOfMessages: 3,
		VisibilityTimeout:   5,
		WaitTimeSeconds:     5,
	}

	h := jobqueue.HandlerFunc(func(ctx context.Context, job *jobqueue.Job) error {
		slog.Info("processing job", slog.Any("job", job))
		var v string
		if err := json.Unmarshal(job.Data, &v); err != nil {
			return err
		}
		slog.Info("job data decoded", slog.String("data", v))
		// time.Sleep(10 * time.Second)
		slog.Info("job processed", slog.Any("job", job))
		return nil
	})

	s := jobqueue.NewServer(d, h)
	s.OnHandlerError = func(err error) {
		slog.Error("error processing message", slog.Any("err", err))
	}
	s.OnDequeuerError = func(err error) {
		slog.Error("error dequeuing message", slog.Any("err", err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, os.Kill)
	defer stop()

	shutdownCompleted := make(chan struct{})
	go func() {
		<-ctx.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.Shutdown(ctx); err != nil {
			slog.Error("error shutting down server", slog.Any("err", err))
		}

		close(shutdownCompleted)
	}()

	if err := s.Serve(); !errors.Is(err, jobqueue.ErrServerShuttedDown) {
		slog.Error("error from server", slog.Any("err", err))
	}

	<-shutdownCompleted
}
