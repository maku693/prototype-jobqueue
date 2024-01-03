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
	return b.handler(job.Kind).Process(ctx, job)
}

func (b *Broker) handler(kind string) Handler {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.handlers[kind]
}

var _ Handler = new(Broker)

type Server struct {
	ProcessTimeout time.Duration
	OnError        func(err error)
	d              Dequeuer
	h              Handler
	wg             sync.WaitGroup
	isShuttedDown  atomic.Bool
	// concurrency
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
		s.handleError(err)
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
			s.handleError(err)
			return
		}

		if err := s.d.Delete(ctx, job); err != nil {
			s.handleError(err)
		}
	}(ctx, job)

	return nil
}

func (s *Server) handleError(err error) {
	if s.OnError == nil {
		return
	}
	s.OnError(err)
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

	if len(d.pendingMessages) == 0 {
		res, err := d.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                new(string),
			AttributeNames:          []types.QueueAttributeName{},
			MaxNumberOfMessages:     d.MaxNumberOfMessages,
			MessageAttributeNames:   []string{},
			ReceiveRequestAttemptId: new(string),
			VisibilityTimeout:       d.VisibilityTimeout,
			WaitTimeSeconds:         d.WaitTimeSeconds,
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

type A struct {
	client              *sqs.Client
	queueUrl            string
	maxNumberOfMessages int32
	visibilityTimeout   int32
	waitTimeSeconds     int32

	onError func(err error)
}

func (d *A) X(ctx context.Context, h Handler) error {
	receiveStartsAt := time.Now()
	res, err := d.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:                new(string),
		AttributeNames:          []types.QueueAttributeName{},
		MaxNumberOfMessages:     d.maxNumberOfMessages,
		MessageAttributeNames:   []string{},
		ReceiveRequestAttemptId: new(string),
		VisibilityTimeout:       d.visibilityTimeout,
		WaitTimeSeconds:         d.waitTimeSeconds,
	})
	if err != nil {
		return err
	}

	var (
		length = len(res.Messages)
		errs   = make([]error, length)
		wg     sync.WaitGroup
	)

	perform := func(ctx context.Context, i int) {
		defer wg.Done()
		msg := res.Messages[i]

		ctx, cancel := context.WithDeadline(ctx, receiveStartsAt.Add(time.Duration(d.visibilityTimeout)*time.Second))
		defer cancel()

		var kind string
		if kindAttr, ok := msg.MessageAttributes["kyu_kind"]; ok {
			kind = *kindAttr.StringValue
		}

		job := &Job{
			Kind: kind,
			Data: []byte(*msg.Body),
		}

		if err := h.Process(ctx, job); err != nil {
			d.onError(err)
			return
		}

		d.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl:      aws.String(d.queueUrl),
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	wg.Add(length)

	for i := range res.Messages {
		go perform(ctx, i)
	}

	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return err
	}

	return nil
}
