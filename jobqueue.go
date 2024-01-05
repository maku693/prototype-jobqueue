package jobqueue

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
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
	ID   string
	Kind string
	Data []byte
}

// type EnqueueResult interface {
// 	ID() string
// }

type Enqueuer interface {
	Enqueue(ctx context.Context, kind string, data []byte, opts *EnqueueOptions) (string, error)
	// Enqueue(ctx context.Context, kind string, data []byte, opts *EnqueueOptions) (EnqueueResult, error)
}

type EnqueueOptions struct {
	PerformAt time.Time
}

var ErrNoJobsEnqueued = errors.New("no jobs enqueued")

// type DequeueResult struct {
// 	Job() *Job
// }

type Dequeuer interface {
	Dequeue(ctx context.Context) (*Job, error)
	// Dequeue(ctx context.Context) (DequeueResult, error)
	Delete(ctx context.Context, jobID string) error
}

type Handler interface {
	Process(ctx context.Context, job *Job) error
}

type HandlerFunc func(ctx context.Context, job *Job) error

func (f HandlerFunc) Process(ctx context.Context, job *Job) error {
	return f(ctx, job)
}

type Middleware func(next Handler) Handler

func WithRecover(next Handler, recoverFunc RecoverFunc) Handler {
	return HandlerFunc(func(ctx context.Context, job *Job) error {
		defer func() {
			if err := recover(); err != nil {
				recoverFunc(ctx, job, err)
			}
		}()
		return next.Process(ctx, job)
	})
}

type RecoverFunc func(ctx context.Context, job *Job, err any)

var _ Handler = HandlerFunc(nil)

type Mux struct {
	handlers map[string]Handler
}

func (m *Mux) Handle(kind string, handler Handler) {
	if _, exists := m.handlers[kind]; exists {
		panic("handler already registered for " + kind)
	}
	if m.handlers == nil {
		m.handlers = make(map[string]Handler)
	}
	m.handlers[kind] = handler
}

func (m *Mux) Process(ctx context.Context, job *Job) error {
	handler, ok := m.handlers[job.Kind]
	if !ok {
		return errors.New("no handler registered for " + job.Kind)
	}
	return handler.Process(ctx, job)
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

var ErrServerShuttedDown = errors.New("server is shutted down")

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

	if s.sem == nil {
		maxConcurrency := s.MaxConcurrency
		if maxConcurrency == 0 {
			maxConcurrency = runtime.NumCPU()
		}
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

		if err := s.Dequeuer.Delete(ctx, job.ID); err != nil {
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

	sleepDuration := time.Millisecond
	timer := time.NewTimer(sleepDuration)

	for {
		select {
		case <-activeJobsCompleted:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			timer.Reset(sleepDuration + sleepDuration)
		}
	}
}

type InMemoryQueue struct {
	mu         sync.RWMutex
	jobs       []*Job
	activeJobs []*Job
	// scheduledJobs []*scheduledJob
}

// type scheduledJob struct {
// 	job       *Job
// 	performAt time.Time
// }

func (q *InMemoryQueue) Enqueue(ctx context.Context, kind string, data []byte, opts *EnqueueOptions) (string, error) {
	idbytes := make([]byte, 16)
	_, err := rand.Read(idbytes)
	if err != nil {
		return "", err
	}
	id := hex.EncodeToString(idbytes)

	job := &Job{
		ID:   id,
		Kind: kind,
		Data: data,
	}

	// TODO: cleanup
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
		return job.ID, nil
	}

	appendJob()

	return job.ID, nil
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

func (q *InMemoryQueue) Delete(ctx context.Context, jobID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.activeJobs = slices.DeleteFunc(q.activeJobs, func(k *Job) bool {
		return k.ID == jobID
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

func (e *SQSEnqueuer) Enqueue(ctx context.Context, kind string, data []byte, opts *EnqueueOptions) (string, error) {
	var delaySeconds int32
	if performAt := opts.PerformAt; !performAt.IsZero() {
		delaySeconds = int32(time.Now().Sub(performAt).Seconds())
		if delaySeconds < 0 {
			delaySeconds = 0
		}
	}

	messageAttrs := make(map[string]types.MessageAttributeValue)
	messageAttrs["jobqueue_kind"] = types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(kind),
	}
	// "jobqueue_id"
	messageBody := base64.StdEncoding.EncodeToString(data)
	sendMessageInput := &sqs.SendMessageInput{
		MessageBody:       aws.String(messageBody),
		QueueUrl:          aws.String(e.QueueUrl),
		DelaySeconds:      delaySeconds,
		MessageAttributes: messageAttrs,
	}

	if strings.HasSuffix(e.QueueUrl, ".fifo") {
		sum := sha256.Sum256(append([]byte(kind), data...))
		msgDedupeId := hex.EncodeToString(sum[:])
		sendMessageInput.MessageDeduplicationId = aws.String(msgDedupeId)

		if e.MessageGroupId != "" {
			sendMessageInput.MessageGroupId = aws.String(e.MessageGroupId)
		}
	}

	res, err := e.Client.SendMessage(ctx, sendMessageInput)
	if err != nil {
		return "", err
	}

	return *res.MessageId, nil
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

	mu               sync.Mutex
	receivedMessages []types.Message
	receiptHandles   map[string]*string
}

func NewSQSDequeuer(client *sqs.Client, queueUrl string) *SQSDequeuer {
	return &SQSDequeuer{
		Client:   client,
		QueueUrl: queueUrl,
	}
}

var ErrMissingKindAttribute = errors.New("missing jobqueue_kind attribute")

func (d *SQSDequeuer) Dequeue(ctx context.Context) (*Job, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// un-batch?
	if len(d.receivedMessages) == 0 {
		attrNames := d.AttributeNames
		if len(attrNames) == 0 {
			attrNames = []types.QueueAttributeName{
				types.QueueAttributeNameAll,
			}
		}

		maxNumberOfMessages := d.MaxNumberOfMessages
		if maxNumberOfMessages == 0 {
			maxNumberOfMessages = 1
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

		d.receivedMessages = res.Messages
	}

	if len(d.receivedMessages) == 0 {
		return nil, ErrNoJobsEnqueued
	}

	msg := d.receivedMessages[0]
	d.receivedMessages = d.receivedMessages[1:]

	kindAttr, ok := msg.MessageAttributes["jobqueue_kind"]
	if !ok {
		return nil, ErrMissingKindAttribute
	}
	kind := *kindAttr.StringValue

	data, err := base64.StdEncoding.DecodeString(*msg.Body)
	if err != nil {
		return nil, err
	}

	job := &Job{
		ID:   *msg.MessageId,
		Kind: kind,
		Data: data,
	}

	if d.receiptHandles == nil {
		d.receiptHandles = make(map[string]*string)
	}
	d.receiptHandles[job.ID] = msg.ReceiptHandle

	return job, nil
}

var ErrReceiptHandleNotFound = errors.New("receipt handle not found")

func (d *SQSDequeuer) Delete(ctx context.Context, jobID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	receiptHandle, ok := d.receiptHandles[jobID]
	if !ok {
		return ErrReceiptHandleNotFound
	}

	_, err := d.Client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(d.QueueUrl),
		ReceiptHandle: receiptHandle,
	})
	if err != nil {
		return err
	}

	delete(d.receiptHandles, jobID)

	return nil
}

var _ Dequeuer = new(SQSDequeuer)
