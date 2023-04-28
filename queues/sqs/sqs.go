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

type Client struct {
	QueueName string
	// desired / default delay durations
	MaxDelay       time.Duration
	ReconnectDelay time.Duration
	ResendDelay    time.Duration
	RoutingKey     string

	isReady bool
	logger  *log.Logger
	// current delay durations
	reconnectDelay time.Duration
	resendDelay    time.Duration

	region    string //TODO: configure region??
	sqsClient *sqs.Client
	sqsURL    *sqs.GetQueueUrlOutput
}

type SQSError struct {
	error
}

var (
	errAlreadyClosed = SQSError{util.WrapError(nil, "already closed: not connected to the server")}
	errShutdown      = SQSError{util.WrapError(nil, "client is shutting down")}
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
	}

	fmt.Println("LoadDefaultConfig")
	// load the default aws config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		log.Fatalf("configuration error: %v", err)
	}

	client := Client{
		MaxDelay:       10 * time.Minute,
		QueueName:      queryMap["queue-name"][0],
		ReconnectDelay: 2 * time.Second,
		ResendDelay:    1 * time.Second,

		logger:    log.New(os.Stdout, "", log.LstdFlags),
		sqsClient: sqs.NewFromConfig(cfg),
	}
	// Get the URL for the queue
	input := &sqs.GetQueueUrlInput{
		QueueName: &queryMap["queue-name"][0],
	}

	sqsURL, err := client.sqsClient.GetQueueUrl(ctx, input)
	if err != nil {
		client.logger.Printf("error getting the queue URL: %v", err)
		return nil, SQSError{util.WrapError(err, fmt.Sprintf("unable to retrieve SQS URL from: %s", urlString))}
	}
	client.sqsURL = sqsURL

	client.reconnectDelay = client.ReconnectDelay
	client.resendDelay = client.ResendDelay
	client.isReady = true
	client.logger.Println("Setup!")
	return &client, nil
}

// ----------------------------------------------------------------------------

// send a message to a queue.
func (client *Client) sendMessage(ctx context.Context, record queues.Record) (err error) {

	// Send a message with attributes to the given queue
	messageInput := &sqs.SendMessageInput{
		DelaySeconds: 0,
		MessageAttributes: map[string]types.MessageAttributeValue{
			"MessageID": {
				DataType:    aws.String("String"),
				StringValue: aws.String(record.GetMessageId()),
			},
		},
		MessageBody: aws.String(record.GetMessage()),
		QueueUrl:    client.sqsURL.QueueUrl,
	}

	resp, err := client.sqsClient.SendMessage(ctx, messageInput)
	if err != nil {
		client.logger.Printf("error sending the message: %v", err)
		return
	}

	client.logger.Printf("AWS response Message ID: %s", *resp.MessageId)

	return nil
}

// ----------------------------------------------------------------------------

// progressively increase the retry delay
func (client *Client) progressiveDelay(delay time.Duration) time.Duration {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	newDelay := delay + time.Duration(r.Intn(int(delay/time.Second)))*time.Second
	if newDelay > client.MaxDelay {
		return client.MaxDelay
	}
	return newDelay
}

// ----------------------------------------------------------------------------

// Push will push data onto the queue and wait for a response.
// TODO: work on resend with delay...
func (client *Client) Push(ctx context.Context, record queues.Record) error {

	if !client.isReady {
		// wait for client to be ready
		// <-client.notifyReady
		return SQSError{util.WrapError(nil, "SQS client is not ready.")}
	}

	for {
		err := client.sendMessage(ctx, record)
		if err != nil {
			client.logger.Println("Push failed. Retrying in", client.resendDelay, ". MessageId:", record.GetMessageId()) //TODO:  debug or trace logging, add messageId
			select {
			case <-ctx.Done():
				return errShutdown
			case <-time.After(client.resendDelay):
				//TODO:  resend forever???
				client.resendDelay = client.progressiveDelay(client.resendDelay)
			}
			continue
			// return err
		} else {
			//reset the resend delay
			client.resendDelay = client.ResendDelay
			return nil
		}
	}
}

// ----------------------------------------------------------------------------

// receive a message from a queue.
func (client *Client) receiveMessage(ctx context.Context) (*sqs.ReceiveMessageOutput, error) {

	// Receive a message with attributes to the given queue
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              client.sqsURL.QueueUrl,
		MessageAttributeNames: []string{"All"},
		MaxNumberOfMessages:   1,
		VisibilityTimeout:     int32(10),
	}

	msg, err := client.sqsClient.ReceiveMessage(ctx, receiveInput)
	if err != nil {
		client.logger.Printf("error receiving messages: %v", err)
		return nil, SQSError{util.WrapError(err, "error receiving messages")}
	}

	if msg.Messages == nil {
		client.logger.Printf("No messages found")
		return nil, SQSError{util.WrapError(nil, "No messages.")}
	}

	client.logger.Printf("Message ID: %s, Message Body: %s", *msg.Messages[0].MessageId, *msg.Messages[0].Body)
	return msg, nil
}

// ----------------------------------------------------------------------------

// Consume will continuously put queue messages on the channel.
func (client *Client) Consume(ctx context.Context) (<-chan *types.Message, error) {
	if !client.isReady {
		// wait for client to be ready
		// <-client.notifyReady
		return nil, SQSError{util.WrapError(nil, "SQS client is not ready.")}
	}
	outChan := make(chan *types.Message)
	go func() {
		for {
			output, err := client.receiveMessage(ctx)
			if err != nil {
				client.logger.Println("receiveMessage failed")
				time.Sleep(client.reconnectDelay)
				client.reconnectDelay = client.progressiveDelay(client.resendDelay)
				continue
			}
			select {
			case <-ctx.Done():
				return
			default:
				for _, m := range output.Messages {
					outChan <- &m
				}
				// reset the reconnectDelay
				client.reconnectDelay = client.ReconnectDelay
			}
		}
	}()
	return outChan, nil
}

// ----------------------------------------------------------------------------

// Remove a message from the SQS queue
func (client *Client) RemoveMessage(ctx context.Context, msg *types.Message) error {
	deleteMessageInput := &sqs.DeleteMessageInput{
		QueueUrl:      client.sqsURL.QueueUrl,
		ReceiptHandle: msg.ReceiptHandle,
	}

	_, err := client.sqsClient.DeleteMessage(ctx, deleteMessageInput)
	if err != nil {
		fmt.Println("Got an error deleting the message:")
		fmt.Println(err)
		return err
	}

	fmt.Println("Deleted message from queue with URL ", *client.sqsURL.QueueUrl)
	return nil
}
