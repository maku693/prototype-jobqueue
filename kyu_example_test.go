package kyu

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	JobKindExample = "Example"
)

func Example() {
	q := &InMemoryQueue{}

	go func() {
		for {
			data, _ := json.Marshal("hello")
			q.Enqueue(
				context.Background(),
				&Job{
					Kind: JobKindExample,
					Data: data,
				},
				&EnqueueOptions{
					PerformAt: time.Now().Add(1 * time.Hour),
				},
			)
		}
	}()

	b := &Broker{}
	b.Handle(
		JobKindExample,
		HandlerFunc(func(ctx context.Context, j *Job) error {
			var v string
			if err := json.Unmarshal(j.Data, &v); err != nil {
				return err
			}
			slog.Info("job processed", slog.String("data", v))
			return nil
		}),
	)

	s := NewServer(q, b)
	go func() {
		s.Serve()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, os.Kill)
	defer stop()

	<-ctx.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := s.Shutdown(ctx); err != nil {
		slog.Error("error shutting down", slog.Any("err", err))
	}
}
