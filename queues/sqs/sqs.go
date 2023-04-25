package sqs

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/go-util/util"
)

type SQSQueueAPI interface {
	GetQueueUrl(ctx context.Context,
		params *sqs.GetQueueUrlInput,
		optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)

	SendMessage(ctx context.Context,
		params *sqs.SendMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

type Client struct {
	QueueName string
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

	sqsClient *sqs.Client
	sqsURL    *sqs.GetQueueUrlOutput
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
func NewClient(ctx context.Context, urlString string) (*Client, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, SQSError{util.WrapError(err, "unable to parse SQS URL string")}
		// panic(err)
	}
	queryMap, _ := url.ParseQuery(u.RawQuery)
	if len(queryMap["queue-name"]) < 1 {
		return nil, SQSError{util.WrapError(err, "please define a queue-name as query parameters")}
		// panic("Please define an exchange and queue-name as query parameters.")
	}

	// load the default aws config along with custom resolver.
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	client := Client{
		QueueName:      queryMap["queue-name"][0],
		ReconnectDelay: 2 * time.Second,
		ReInitDelay:    2 * time.Second,
		ResendDelay:    1 * time.Second,

		done:        make(chan bool),
		logger:      log.New(os.Stdout, "", log.LstdFlags),
		notifyReady: make(chan interface{}),
		sqsClient:   sqs.NewFromConfig(cfg),
	}
	// Get the URL for the queue
	input := &sqs.GetQueueUrlInput{
		QueueName: &queryMap["queue-name"][0],
	}
	sqsURL, err := GetQueueURL(ctx, client.sqsClient, input)
	if err != nil {
		log.Printf("error getting the queue URL: %v", err)
		return nil, SQSError{util.WrapError(err, fmt.Sprintf("unable to retrieve SQS URL from: %s", urlString))}
	}
	client.sqsURL = sqsURL

	client.reconnectDelay = client.ReconnectDelay
	client.reInitDelay = client.ReInitDelay
	client.resendDelay = client.ResendDelay

	return &client, nil
}

func GetQueueURL(c context.Context, api SQSQueueAPI, input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	return api.GetQueueUrl(c, input)
}

func SendMessage(c context.Context, api SQSQueueAPI, input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	return api.SendMessage(c, input)
}

// send a message to a queue.
func (client *Client) sendMessage(ctx context.Context, record queues.Record) (err error) {

	// Send a message with attributes to the given queue
	messageInput := &sqs.SendMessageInput{
		DelaySeconds: 0,
		MessageAttributes: map[string]types.MessageAttributeValue{
			//file name or url of the source data
			"Source": {
				DataType:    aws.String("String"),
				StringValue: aws.String("filename or url"),
			},
			//line number or numeric id of the source record
			"ID": {
				DataType:    aws.String("Number"),
				StringValue: aws.String("10"),
			},
		},
		MessageBody: aws.String("article about sending a message to AWS SQS using Go"),
		QueueUrl:    client.sqsURL.QueueUrl,
	}

	resp, err := SendMessage(ctx, client.sqsClient, messageInput)
	if err != nil {
		log.Printf("error sending the message: %v", err)
		return
	}

	log.Printf("Message ID: %s", *resp.MessageId)
	log.Println(resp.ResultMetadata)
	return nil
}

// ----------------------------------------------------------------------------

// progressively increase the retry delay
func (client *Client) progressiveDelay(delay time.Duration) time.Duration {
	//TODO:  seed random number generator
	return delay + time.Duration(rand.Intn(int(delay/time.Second)))*time.Second
}

// ----------------------------------------------------------------------------

// Push will push data onto the queue and wait for a confirm.
// If no confirm is received by the resendTimeout,
// it re-sends messages until a confirm is received.
// This will block until the server sends a confirm. Errors are
// only returned if the push action itself fails, see UnsafePush.
func (client *Client) Push(ctx context.Context, record queues.Record) error {

	if !client.isReady {
		// wait for client to be ready
		<-client.notifyReady
	}

	// for {
	err := client.sendMessage(ctx, record)
	if err != nil {
		client.logger.Println("Push failed. Retrying in", client.resendDelay, ". MessageId:", record.GetMessageId()) //TODO:  debug or trace logging, add messageId
		select {
		case <-client.done:
			return errShutdown //TODO:  error message to include messageId?
		case <-time.After(client.resendDelay):
			client.resendDelay = client.progressiveDelay(client.resendDelay)
		}
		// continue
		return err
	}
	// select {
	// case confirm := <-client.notifyConfirm:
	// 	if confirm.Ack {
	// 		// reset resend delay
	// 		client.resendDelay = client.ResendDelay
	// 		return nil
	// 	}
	// case <-time.After(client.resendDelay):
	// 	client.resendDelay = client.progressiveDelay(client.resendDelay)
	// }
	// client.logger.Println("Push didn't confirm. Retrying in", client.resendDelay, ". MessageId:", record.GetMessageId()) //TODO:  debug or trace logging, add messageId
	// }
	return nil
}
