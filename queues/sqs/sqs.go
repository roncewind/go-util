package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/roncewind/go-util/queues"
	"github.com/roncewind/go-util/util"
)

type Client struct {
	DeadLetterQueueURL string
	QueueName          string
	QueueURL           *string
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

	region       string //: configure region??
	sqsClient    *sqs.Client
	sqsDLQClient *sqs.Client
	sqsURL       *sqs.GetQueueUrlOutput
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
	client := Client{
		MaxDelay:       10 * time.Minute,
		ReconnectDelay: 2 * time.Second,
		ResendDelay:    1 * time.Second,

		logger: log.New(os.Stdout, "", log.LstdFlags),
	}
	// load the default aws config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return &client, err
	}
	client.sqsClient = sqs.NewFromConfig(cfg)

	err = client.getQueueURL(ctx, urlString)
	if err != nil {
		return &client, err
	}

	client.reconnectDelay = client.ReconnectDelay
	client.resendDelay = client.ResendDelay
	client.getRedrivePolicy(ctx)
	client.isReady = true
	client.logger.Println("QueueURL:", *client.QueueURL)
	client.logger.Println("dead letter queue URL:", client.DeadLetterQueueURL)
	client.logger.Println("Setup!")
	return &client, nil
}

// ----------------------------------------------------------------------------

// Interrogates the given input URL and retrieves the SQS URL or uses it as it.
func (client *Client) getQueueURL(ctx context.Context, urlString string) error {
	u, err := url.Parse(urlString)
	if err != nil {
		return SQSError{util.WrapError(err, "unable to parse SQS URL string")}
		// panic(err)
	}
	queryMap, _ := url.ParseQuery(u.RawQuery)
	if len(queryMap["queue-name"]) < 1 {
		client.QueueURL = &urlString
	} else {
		client.QueueName = queryMap["queue-name"][0]
		// Get the URL for the queue
		input := &sqs.GetQueueUrlInput{
			QueueName: &client.QueueName,
		}
		sqsURL, err := client.sqsClient.GetQueueUrl(ctx, input)
		if err != nil {
			client.logger.Printf("error getting the queue URL: %v", err)
			return SQSError{util.WrapError(err, fmt.Sprintf("unable to retrieve SQS URL from: %s", urlString))}
		}
		client.sqsURL = sqsURL
		client.QueueURL = sqsURL.QueueUrl
	}
	return nil
}

// ----------------------------------------------------------------------------

// Pulls the SQS queues Redrive Policy from AWS and sets the dead letter
// queue URL in the client
func (client *Client) getRedrivePolicy(ctx context.Context) {
	params := &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(*client.QueueURL),
		AttributeNames: []types.QueueAttributeName{
			// types.QueueAttributeNameAll,
			types.QueueAttributeNameRedrivePolicy,
		},
	}
	queueAttributes, err := client.sqsClient.GetQueueAttributes(ctx, params)
	if err != nil {
		client.logger.Println("Unable to retrieve queue redrive policy", err)
		return
	}
	redrive := queueAttributes.Attributes[string(types.QueueAttributeNameRedrivePolicy)]
	var redrivePolicy redrivePolicy
	err = json.Unmarshal([]byte(redrive), &redrivePolicy)
	if err != nil {
		client.logger.Println("error unmarshalling redrive policy", err)
		return
	}
	fields := strings.Split(redrivePolicy.DeadLetterTargetArn, ":")
	client.DeadLetterQueueURL = fmt.Sprintf("https://queue.amazonaws.com/%s/%s", fields[4], fields[5])
	if len(client.DeadLetterQueueURL) > 0 {
		// load the default aws config
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			client.sqsDLQClient = nil
		} else {
			client.sqsDLQClient = sqs.NewFromConfig(cfg)
		}
	}
}

// ----------------------------------------------------------------------------

// internal struct to parse the AWS Redrive Policy attribute JSON
type redrivePolicy struct {
	DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	MaxReceiveCount     int    `json:"maxReceiveCount"`
}

// ----------------------------------------------------------------------------

// send a message to a queue.
func (client *Client) sendDeadRecord(ctx context.Context, record types.Message) (err error) {

	if client.sqsDLQClient == nil {
		return errors.New("No dead letter queue found")
	}

	// Send a message with attributes to the given queue
	messageInput := &sqs.SendMessageInput{
		DelaySeconds:      0,
		MessageAttributes: record.MessageAttributes,
		MessageBody:       aws.String(*record.Body),
		QueueUrl:          &client.DeadLetterQueueURL,
	}

	resp, err := client.sqsDLQClient.SendMessage(ctx, messageInput)
	if err != nil {
		client.logger.Printf("error sending the dead record: %v", err)
		return
	}

	client.logger.Printf("AWS response Message ID: %s", *resp.MessageId)

	return nil
}

// ----------------------------------------------------------------------------

// send a message to a queue.
func (client *Client) sendRecord(ctx context.Context, record queues.Record) (err error) {

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
		QueueUrl:    client.QueueURL,
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

// send a message to a queue.
func (client *Client) sendRecordBatch(ctx context.Context, records []queues.Record) (err error) {
	var messages []types.SendMessageBatchRequestEntry
	messages = make([]types.SendMessageBatchRequestEntry, len(records))
	r := rand.New(rand.NewSource(time.Now().Unix()))
	id := r.Intn(10000)
	i := 0
	for _, record := range records {
		if record != nil {
			messages[i] = types.SendMessageBatchRequestEntry{
				DelaySeconds: 0,
				Id:           aws.String(fmt.Sprintf("%d", id+i)),
				MessageAttributes: map[string]types.MessageAttributeValue{
					"MessageID": {
						DataType:    aws.String("String"),
						StringValue: aws.String(record.GetMessageId()),
					},
				},
				MessageBody: aws.String(record.GetMessage()), //?  aws.String(string(utils.Base64Encode([]byte(body)))),
			}
			i++
		}
	}
	// bail of we have no messages to send
	if i <= 0 {
		return
	}
	// Send a message with attributes to the given queue
	messageInput := &sqs.SendMessageBatchInput{
		Entries:  messages[0:i],
		QueueUrl: client.QueueURL,
	}

	resp, err := client.sqsClient.SendMessageBatch(ctx, messageInput)
	if err != nil {
		client.logger.Printf("error sending the message batch: %v", err)
	}
	if resp != nil {
		if len(resp.Failed) > 0 {
			for _, fail := range resp.Failed {
				client.logger.Println("error sending the message in batch:", fail.Message)
				client.logger.Println("message id:", fail.Id)
			}
		}
		client.logger.Println("Successfully sent:", len(resp.Successful), "messages")
	}

	return
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

// PushDeadRecord will push an erroneous record onto the DLQ.
// TODO: work on resend with delay...
func (client *Client) PushDeadRecord(ctx context.Context, record types.Message) error {
	return client.sendDeadRecord(ctx, record)
}

// ----------------------------------------------------------------------------

// Push will push data onto the queue and wait for a response.
// TODO: work on resend with delay...
func (client *Client) Push(ctx context.Context, record queues.Record) error {

	if !client.isReady {
		return SQSError{util.WrapError(nil, "SQS client is not ready.")}
	}

	for {
		err := client.sendRecord(ctx, record)
		if err != nil {
			client.logger.Println("Push failed. Retrying in", client.resendDelay, ". MessageId:", record.GetMessageId()) //:  debug or trace logging, add messageId
			select {
			case <-ctx.Done():
				return errShutdown
			case <-time.After(client.resendDelay):
				//:  resend forever???
				client.resendDelay = client.progressiveDelay(client.resendDelay)
			}
			continue
		} else {
			//reset the resend delay
			client.resendDelay = client.ResendDelay
			return nil
		}
	}
}

// ----------------------------------------------------------------------------

// Push will push data onto the queue and wait for a response.
// TODO: work on resend with delay????
func (client *Client) PushBatch(ctx context.Context, recordchan <-chan queues.Record) error {

	if !client.isReady {
		// wait for client to be ready
		// <-client.notifyReady
		return SQSError{util.WrapError(nil, "SQS client is not ready.")}
	}
	i := 0
	records := make([]queues.Record, 10)
	for record := range util.OrDone(ctx, recordchan) {
		records[i] = record
		fmt.Println("batch push record:!", record.GetMessageId())
		i++
		if i >= 10 {
			err := client.sendRecordBatch(ctx, records)
			if err != nil {
				client.logger.Println("sendRecordBatch error:", err)
			}
			i = 0
			records = make([]queues.Record, 10)
		}
	}
	//handle a last partial batch
	if i > 0 {
		err := client.sendRecordBatch(ctx, records)
		if err != nil {
			client.logger.Println("last batch, sendRecordBatch error:", err)
		}
	}
	return nil
}

// ----------------------------------------------------------------------------

// receive a message from a queue.
func (client *Client) receiveMessage(ctx context.Context, visibilitySeconds int32) (*sqs.ReceiveMessageOutput, error) {

	// Receive a message with attributes to the given queue
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              client.QueueURL,
		MessageAttributeNames: []string{"All"},
		MaxNumberOfMessages:   10,
		VisibilityTimeout:     visibilitySeconds,
	}

	msg, err := client.sqsClient.ReceiveMessage(ctx, receiveInput)
	if err != nil {
		client.logger.Printf("error receiving messages: %v", err)
		return nil, SQSError{util.WrapError(err, "error receiving messages")}
	}
	if msg.Messages == nil || len(msg.Messages) <= 0 {
		client.logger.Printf("No messages found")
		return nil, SQSError{util.WrapError(nil, "No messages.")}
	}

	return msg, nil
}

// ----------------------------------------------------------------------------

// Consume will continuously put queue messages on the channel.
func (client *Client) Consume(ctx context.Context, visibilitySeconds int32) (<-chan types.Message, error) {
	if !client.isReady {
		return nil, SQSError{util.WrapError(nil, "SQS client is not ready.")}
	}
	outChan := make(chan types.Message, 10)
	go func() {
		messageCount := 0
		for {
			output, err := client.receiveMessage(ctx, visibilitySeconds)

			if err != nil {
				time.Sleep(client.reconnectDelay)
				client.reconnectDelay = client.progressiveDelay(client.reconnectDelay)
			} else {
				select {
				case <-ctx.Done():
					fmt.Println("DEBUG: final messageCount:", messageCount)
					return
				default:
					for _, m := range output.Messages {
						outChan <- m
						messageCount++
						fmt.Println("DEBUG: read message from SQS. messageCount:", messageCount)
					}
					// reset the reconnectDelay
					client.reconnectDelay = client.ReconnectDelay
				}
			}
		}
	}()
	return outChan, nil
}

// ----------------------------------------------------------------------------

// Remove a message from the SQS queue
func (client *Client) RemoveMessage(ctx context.Context, msg types.Message) error {
	deleteMessageInput := &sqs.DeleteMessageInput{
		QueueUrl:      client.QueueURL,
		ReceiptHandle: msg.ReceiptHandle,
	}

	fmt.Println("SQS Client delete message:", msg.MessageId)
	_, err := client.sqsClient.DeleteMessage(ctx, deleteMessageInput)
	if err != nil {
		client.logger.Println("Got an error deleting the message:")
		client.logger.Println(err)
		return err
	}
	return nil
}

// ----------------------------------------------------------------------------

// Remove a message from the SQS queue
func (client *Client) SetMessageVisibility(ctx context.Context, msg types.Message, seconds int32) error {

	if seconds < 0 {
		seconds = 0
	}

	if seconds > 12*60*60 { //12 hour max
		seconds = 12 * 60 * 60
	}

	setVisibilityInput := &sqs.ChangeMessageVisibilityInput{
		ReceiptHandle:     msg.ReceiptHandle,
		QueueUrl:          client.QueueURL,
		VisibilityTimeout: seconds,
	}

	fmt.Println("SQS Client set message visibility:", msg.MessageId)
	_, err := client.sqsClient.ChangeMessageVisibility(ctx, setVisibilityInput)
	if err != nil {
		client.logger.Println("Got an error changing message visibility:")
		client.logger.Println(err)
		return err
	}
	return nil
}

// ----------------------------------------------------------------------------

// Close will cleanly shutdown the channel and connection.
func (client *Client) Close() error {
	return nil
}
