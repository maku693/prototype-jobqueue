package kyu

import "context"

type Queue interface {
	SendMessage(ctx context.Context, body MessageBody) error
	ReceiveMessage(ctx context.Context) (Message, error)
	DeleteMessage(ctx context.Context, msgid MessageID) error
}

type InMemoryQueue struct {
}
