package main

import (
	"context"
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
			kind := JobKindExample
			data := fmt.Sprintf("hello (%d)", i)
			slog.Info("enqueueing job", slog.String("kind", kind), slog.Any("data", data))
			q.Enqueue(
				context.Background(),
				JobKindExample,
				data,
				&jobqueue.EnqueueOptions{
					PerformAt: time.Now().Add(5 * time.Second),
				},
			)
			// slog.Info("job enqueued", slog.Any("id", id))

			time.Sleep(1 * time.Second)
		}
	}()

	m := &jobqueue.Mux{}
	m.Handle(
		JobKindExample,
		jobqueue.HandlerFunc(func(ctx context.Context, job *jobqueue.Job) error {
			slog.Info("processing job", slog.Any("job", job))
			// time.Sleep(10 * time.Second)
			slog.Info("job processed", slog.Any("job", job))
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
