package kyu

import "context"

type Worker struct {
	q        Queue
	handlers []Handler
}

func NewWorker(q Queue) *Worker {
	return &Worker{
		q: q,
		handlers: ,
	}
}

type Handler any
