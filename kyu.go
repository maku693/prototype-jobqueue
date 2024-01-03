package kyu

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type Job struct {
	Kind string
	Data []byte
}

type Enqueuer interface {
	Enqueue(ctx context.Context, job *Job, opts *EnqueueOptions) error
}

type EnqueueOptions struct {
	PerformAt time.Time
}

var (
	ErrNoJobsEnqueued = errors.New("no jobs enqueued")
)

type Dequeuer interface {
	Dequeue(ctx context.Context) (*Job, error)
	Delete(ctx context.Context, job *Job) error
}

type Handler interface {
	Process(ctx context.Context, job *Job) error
}

type HandlerFunc func(ctx context.Context, job *Job) error

func (f HandlerFunc) Process(ctx context.Context, job *Job) error {
	return f(ctx, job)
}

var _ Handler = HandlerFunc(nil)

type Broker struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func (b *Broker) Handle(kind string, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.handlers[kind]; exists {
		panic("handler already exists for " + kind)
	}
	if b.handlers == nil {
		b.handlers = make(map[string]Handler)
	}
	b.handlers[kind] = handler
}

func (b *Broker) Process(ctx context.Context, job *Job) error {
	handler := b.handler(job.Kind)
	return handler.Process(ctx, job)
}

func (b *Broker) handler(kind string) Handler {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.handlers[kind]
}

var _ Handler = new(Broker)

type Server struct {
	MaxConcurrency int
	ProcessTimeout time.Duration
	OnHandlerError func(err error)

	// TODO: publicize these

	d Dequeuer
	h Handler

	wg            sync.WaitGroup
	isShuttedDown atomic.Bool
}

func NewServer(d Dequeuer, h Handler) *Server {
	return &Server{
		d: d,
		h: h,
	}
}

var (
	ErrServerShuttedDown = errors.New("server is shutted down")
)

func (s *Server) Serve() error {
	for {
		err := s.process()
		if errors.Is(err, ErrNoJobsEnqueued) {
			time.Sleep(time.Millisecond)
			continue
		}
		if err != nil {
			return err
		}
	}
}

func (s *Server) process() error {
	if s.isShuttedDown.Load() {
		return ErrServerShuttedDown
	}

	ctx := context.Background()

	job, err := s.d.Dequeue(ctx)
	if errors.Is(err, ErrNoJobsEnqueued) {
		return err
	}
	if err != nil {
		s.onHandlerError(err)
		return nil
	}

	var deadline time.Time
	if s.ProcessTimeout != 0 {
		deadline = time.Now().Add(s.ProcessTimeout)
	}

	s.wg.Add(1)
	go func(ctx context.Context, job *Job) {
		defer s.wg.Done()

		if !deadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}

		if err := s.h.Process(ctx, job); err != nil {
			s.onHandlerError(err)
			return
		}

		if err := s.d.Delete(ctx, job); err != nil {
			s.onHandlerError(err)
		}
	}(ctx, job)

	return nil
}

func (s *Server) onHandlerError(err error) {
	if s.OnHandlerError == nil {
		return
	}
	s.OnHandlerError(err)
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.isShuttedDown.Store(true)

	activeJobsCompleted := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(activeJobsCompleted)
	}()

	for {
		select {
		case <-activeJobsCompleted:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

type InMemoryQueue struct {
	mu           sync.RWMutex
	jobs         []*Job
	dequeuedJobs []*Job
}

func (q *InMemoryQueue) Enqueue(ctx context.Context, job *Job, opts *EnqueueOptions) error {
	appendJob := func() {
		q.mu.Lock()
		defer q.mu.Unlock()

		q.jobs = append(q.jobs, job)
	}

	if !opts.PerformAt.IsZero() {
		timer := time.NewTimer(opts.PerformAt.Sub(time.Now()))
		go func() {
			<-timer.C
			appendJob()
		}()
		return nil
	}

	appendJob()

	return nil
}

func (q *InMemoryQueue) Dequeue(ctx context.Context) (*Job, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return nil, ErrNoJobsEnqueued
	}
	job := q.jobs[0]
	q.jobs = q.jobs[1:]
	q.dequeuedJobs = append(q.dequeuedJobs, job)

	return job, nil
}

func (q *InMemoryQueue) Delete(ctx context.Context, j *Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.dequeuedJobs = slices.DeleteFunc(q.dequeuedJobs, func(k *Job) bool {
		return k == j
	})
	return nil
}

var _ interface {
	Enqueuer
	Dequeuer
} = new(InMemoryQueue)

// ---

type SQSEnqueuer struct {
	Client   *sqs.Client
	QueueUrl string
}

func NewSQSEnqueuer(client *sqs.Client, queueUrl string) *SQSEnqueuer {
	return &SQSEnqueuer{
		Client:   client,
		QueueUrl: queueUrl,
	}
}

func (e *SQSEnqueuer) Enqueue(ctx context.Context, job *Job, opts *EnqueueOptions) error {
	messageBody := string(job.Data)

	var delaySeconds int32
	if performAt := opts.PerformAt; !performAt.IsZero() {
		delaySeconds = int32(time.Now().Sub(performAt).Seconds())
		if delaySeconds < 0 {
			delaySeconds = 0
		}
	}

	messageAttrs := make(map[string]types.MessageAttributeValue)
	messageAttrs["kyu_kind"] = types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(job.Kind),
	}

	sendMessageInput := &sqs.SendMessageInput{
		MessageBody:       aws.String(messageBody),
		QueueUrl:          aws.String(e.QueueUrl),
		DelaySeconds:      delaySeconds,
		MessageAttributes: messageAttrs,
	}

	// TODO:
	// if fifo?
	// 	dedupe_id
	// 	group_id

	_, err := e.Client.SendMessage(ctx, sendMessageInput)
	if err != nil {
		return err
	}

	return nil
}

var _ Enqueuer = new(SQSEnqueuer)

type SQSDequeuer struct {
	Client              *sqs.Client
	QueueUrl            string
	MaxNumberOfMessages int32
	VisibilityTimeout   int32
	WaitTimeSeconds     int32

	mu              sync.Mutex
	pendingMessages []types.Message
	activeMessages  map[*Job]types.Message
}

func NewSQSDequeuer() *SQSDequeuer {
	panic("unimplemented")
}

var (
	ErrMissingKyuKindAttribute = errors.New("missing kyu_kind attribute")
)

func (d *SQSDequeuer) Dequeue(ctx context.Context) (*Job, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// TODO: defaulting
	maxNumberOfMessages := d.MaxNumberOfMessages
	visibilityTimeout := d.VisibilityTimeout
	waitTimeSeconds := d.WaitTimeSeconds

	// TODO: FIFO

	if len(d.pendingMessages) == 0 {
		res, err := d.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                new(string),
			AttributeNames:          []types.QueueAttributeName{},
			MaxNumberOfMessages:     maxNumberOfMessages,
			MessageAttributeNames:   []string{},
			ReceiveRequestAttemptId: new(string),
			VisibilityTimeout:       visibilityTimeout,
			WaitTimeSeconds:         waitTimeSeconds,
		})
		if err != nil {
			return nil, err
		}

		d.pendingMessages = res.Messages
	}

	if len(d.pendingMessages) == 0 {
		return nil, ErrNoJobsEnqueued
	}

	msg := d.pendingMessages[0]
	d.pendingMessages = d.pendingMessages[1:]

	kindAttr, ok := msg.MessageAttributes["kyu_kind"]
	if !ok {
		return nil, ErrMissingKyuKindAttribute
	}

	kind := *kindAttr.StringValue
	job := &Job{
		Kind: kind,
		Data: []byte(*msg.Body),
	}

	d.activeMessages[job] = msg

	return job, nil
}

func (d *SQSDequeuer) Delete(ctx context.Context, job *Job) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	msg := d.activeMessages[job]

	_, err := d.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(d.QueueUrl),
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		return err
	}

	delete(d.activeMessages, job)

	return nil
}

var _ Dequeuer = new(SQSDequeuer)
