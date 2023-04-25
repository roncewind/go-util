package sqs

import (
	"context"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/roncewind/go-util/util"
)

type Client struct {
	ExchangeName string
	QueueName    string
	// desired / default delay durations
	ReconnectDelay time.Duration
	ReInitDelay    time.Duration
	ResendDelay    time.Duration
	RoutingKey     string

	done        chan bool
	isReady     bool
	logger      *log.Logger
	notifyReady chan interface{}
	// current delay durations
	reconnectDelay time.Duration
	reInitDelay    time.Duration
	resendDelay    time.Duration
}

type SQSError struct {
	error
}

var (
	errAlreadyClosed = SQSError{util.WrapError(nil, "already closed: not connected to the server")}
	errShutdown      = SQSError{util.WrapError(nil, "client is shutting down")}
	// errAlreadyClosed = errors.New("already closed: not connected to the server")
	// errShutdown      = errors.New("client is shutting down")
)

// ----------------------------------------------------------------------------

// New creates a single SQS client
func NewClient(ctx context.Context, urlString string) *sqs.Client {
	// load the default aws config along with custom resolver.
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	return sqs.NewFromConfig(cfg)
}
