package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/maku693/jobqueue"
)

const (
	JobKindExample = "Example"
)

func main() {
	q := &jobqueue.InMemoryQueue{}

	go func() {
		for i := 0; i < 10; i++ {
			data, _ := json.Marshal(fmt.Sprintf("hello (%d)", i))
			job := &jobqueue.Job{
				Kind: JobKindExample,
				Data: data,
			}
			slog.Info("enqueueing job", slog.Any("job", job))
			q.Enqueue(
				context.Background(),
				job,
				&jobqueue.EnqueueOptions{
					PerformAt: time.Now().Add(5 * time.Second),
				},
			)
			slog.Info("job enqueued", slog.Any("job", job))

			time.Sleep(1 * time.Second)
		}
	}()

	m := &jobqueue.Mux{}
	m.Handle(
		JobKindExample,
		jobqueue.HandlerFunc(func(ctx context.Context, job *jobqueue.Job) error {
			slog.Info("processing job", slog.Any("job", job))
			// time.Sleep(10 * time.Second)
			var v string
			if err := json.Unmarshal(job.Data, &v); err != nil {
				return err
			}
			slog.Info("job processed", slog.String("data", v))
			return nil
		}),
	)

	s := jobqueue.NewServer(q, m)
	s.OnHandlerError = func(err error) {
		slog.Error("error processing message", slog.Any("err", err))
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
