package kyu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"runtime"
	"slices"
	"strings"
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

type Middleware func(next Handler) Handler

var _ Handler = HandlerFunc(nil)

type Mux struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

func (m *Mux) Handle(kind string, handler Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handlers[kind]; exists {
		panic("handler already exists for " + kind)
	}
	if m.handlers == nil {
		m.handlers = make(map[string]Handler)
	}
	m.handlers[kind] = handler
}

func (m *Mux) Process(ctx context.Context, job *Job) error {
	handler := m.handler(job.Kind)
	return handler.Process(ctx, job)
}

func (m *Mux) handler(kind string) Handler {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.handlers[kind]
}

var _ Handler = new(Mux)

type Server struct {
	MaxConcurrency int
	ProcessTimeout time.Duration

	Dequeuer        Dequeuer
	Handler         Handler
	OnDequeuerError func(err error)
	OnHandlerError  func(err error)

	sem           chan struct{}
	wg            sync.WaitGroup
	isShuttedDown atomic.Bool
}

func NewServer(d Dequeuer, h Handler) *Server {
	return &Server{
		Dequeuer: d,
		Handler:  h,
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

	if s.Dequeuer == nil {
		panic("dequeuer is not set")
	}

	job, err := s.Dequeuer.Dequeue(ctx)
	if errors.Is(err, ErrNoJobsEnqueued) {
		return err
	}
	if err != nil {
		s.onDequeuerError(err)
		return nil
	}

	maxConcurrency := s.MaxConcurrency
	if maxConcurrency == 0 {
		maxConcurrency = runtime.NumCPU()
	}
	if s.sem == nil {
		s.sem = make(chan struct{}, maxConcurrency)
	}

	var deadline time.Time
	if s.ProcessTimeout != 0 {
		deadline = time.Now().Add(s.ProcessTimeout)
	}

	s.sem <- struct{}{}
	s.wg.Add(1)
	go func(ctx context.Context, job *Job) {
		defer func() {
			s.wg.Done()
			<-s.sem
		}()

		if !deadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, deadline)
			defer cancel()
		}

		if s.Handler == nil {
			panic("handler is not set")
		}
		if err := s.Handler.Process(ctx, job); err != nil {
			s.onHandlerError(err)
			return
		}

		if err := s.Dequeuer.Delete(ctx, job); err != nil {
			s.onDequeuerError(err)
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

func (s *Server) onDequeuerError(err error) {
	if s.OnDequeuerError == nil {
		return
	}
	s.OnDequeuerError(err)
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
	mu         sync.RWMutex
	jobs       []*Job
	activeJobs []*Job
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
	q.activeJobs = append(q.activeJobs, job)

	return job, nil
}

func (q *InMemoryQueue) Delete(ctx context.Context, j *Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.activeJobs = slices.DeleteFunc(q.activeJobs, func(k *Job) bool {
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

	MessageGroupId string
}

func NewSQSEnqueuer(client *sqs.Client, queueUrl string) *SQSEnqueuer {
	return &SQSEnqueuer{
		Client:   client,
		QueueUrl: queueUrl,
	}
}

func (e *SQSEnqueuer) Enqueue(ctx context.Context, job *Job, opts *EnqueueOptions) error {
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
		MessageBody:       aws.String(string(job.Data)),
		QueueUrl:          aws.String(e.QueueUrl),
		DelaySeconds:      delaySeconds,
		MessageAttributes: messageAttrs,
	}

	if strings.HasSuffix(e.QueueUrl, ".fifo") {
		sum := sha256.Sum256(job.Data)
		msgDedupeId := hex.EncodeToString(sum[:])
		sendMessageInput.MessageDeduplicationId = aws.String(msgDedupeId)

		if e.MessageGroupId != "" {
			sendMessageInput.MessageGroupId = aws.String(e.MessageGroupId)
		}
	}

	_, err := e.Client.SendMessage(ctx, sendMessageInput)
	if err != nil {
		return err
	}

	return nil
}

var _ Enqueuer = new(SQSEnqueuer)

type SQSDequeuer struct {
	Client   *sqs.Client
	QueueUrl string

	AttributeNames        []types.QueueAttributeName
	MaxNumberOfMessages   int32
	MessageAttributeNames []string
	VisibilityTimeout     int32
	WaitTimeSeconds       int32

	mu              sync.Mutex
	pendingMessages []types.Message
	activeMessages  map[*Job]types.Message
}

func NewSQSDequeuer(client *sqs.Client, queueUrl string) *SQSDequeuer {
	return &SQSDequeuer{
		Client:   client,
		QueueUrl: queueUrl,
	}
}

var (
	ErrMissingKyuKindAttribute = errors.New("missing kyu_kind attribute")
)

func (d *SQSDequeuer) Dequeue(ctx context.Context) (*Job, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.pendingMessages) == 0 {
		attrNames := d.AttributeNames
		if len(attrNames) == 0 {
			attrNames = []types.QueueAttributeName{
				types.QueueAttributeNameAll,
			}
		}

		maxNumberOfMessages := d.MaxNumberOfMessages
		if maxNumberOfMessages == 0 {
			maxNumberOfMessages = 10
		}

		msgAttrNames := d.MessageAttributeNames
		if len(msgAttrNames) == 0 {
			msgAttrNames = []string{"All"}
		}

		visibilityTimeout := d.VisibilityTimeout

		waitTimeSeconds := d.WaitTimeSeconds
		if waitTimeSeconds == 0 {
			waitTimeSeconds = 20
		}

		res, err := d.Client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(d.QueueUrl),
			AttributeNames:        attrNames,
			MaxNumberOfMessages:   maxNumberOfMessages,
			MessageAttributeNames: msgAttrNames,
			VisibilityTimeout:     visibilityTimeout,
			WaitTimeSeconds:       waitTimeSeconds,
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

	if d.activeMessages == nil {
		d.activeMessages = make(map[*Job]types.Message)
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
