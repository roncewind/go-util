package rabbitmq

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/url"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	ExchangeName   string
	QueueName      string
	ReconnectDelay time.Duration
	ReInitDelay    time.Duration
	ResendDelay    time.Duration
	RoutingKey     string

	connection      *amqp.Connection
	channel         *amqp.Channel
	done            chan bool
	isReady         bool
	logger          *log.Logger
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	notifyReady     chan interface{}
	reconnectDelay  time.Duration
	reInitDelay     time.Duration
	resendDelay     time.Duration
}

type Record interface {
	GetMessage() string
	GetMessageId() string
}

var (
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("client is shutting down")
)

// ----------------------------------------------------------------------------

// New creates a single RabbitMQ client that will automatically
// attempt to connect to the server.  Reconnection delays are set to defaults.
func NewClient(urlString string) *Client {

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	queryMap, _ := url.ParseQuery(u.RawQuery)
	if len(queryMap["exchange"]) < 1 || len(queryMap["queue-name"]) < 1 {
		panic("Please define an exchange and queue-name as query parameters.")
	}
	routingKey := queryMap["queue-name"][0]
	if len(queryMap["routing-key"]) > 0 {
		routingKey = queryMap["routing-key"][0]
	}

	client := Client{
		ExchangeName:   queryMap["exchange"][0],
		QueueName:      queryMap["queue-name"][0],
		ReconnectDelay: 2 * time.Second,
		ReInitDelay:    2 * time.Second,
		ResendDelay:    1 * time.Second,
		RoutingKey:     routingKey,

		done:        make(chan bool),
		logger:      log.New(os.Stdout, "", log.LstdFlags),
		notifyReady: make(chan interface{}),
	}
	client.reconnectDelay = client.ReconnectDelay
	client.reInitDelay = client.ReInitDelay
	client.resendDelay = client.ResendDelay
	go client.handleReconnect(urlString)
	return &client
}

// ----------------------------------------------------------------------------

// Init initializes a single RabbitMQ client that will automatically
// attempt to connect to the server.
// TODO:  error if required fields (exchangeName, queueName, and urlString aren't specified)
func Init(client *Client, urlString string) *Client {

	// set up defaults if none provided
	if client.ReconnectDelay <= 0 {
		client.ReconnectDelay = 2 * time.Second
	}
	if client.ReInitDelay <= 0 {
		client.ReInitDelay = 2 * time.Second
	}
	if client.ResendDelay <= 0 {
		client.ResendDelay = 1 * time.Second
	}

	// set up internals
	client.done = make(chan bool)
	client.logger = log.New(os.Stdout, "", log.LstdFlags)
	client.notifyReady = make(chan interface{})

	client.reconnectDelay = client.ReconnectDelay
	client.reInitDelay = client.ReInitDelay
	client.resendDelay = client.ResendDelay
	go client.handleReconnect(urlString)
	return client
}

// ----------------------------------------------------------------------------

// handleReconnect will wait for a connection error on
// notifyConnClose, and then continuously attempt to reconnect.
func (client *Client) handleReconnect(addr string) {
	for {
		client.isReady = false
		client.logger.Println("Attempting to connect")

		conn, err := client.connect(addr)

		if err != nil {
			client.logger.Println("Connect error:", err)
			client.logger.Println("Failed to connect. Retrying in", client.reconnectDelay)

			select {
			case <-client.done:
				client.logger.Println("done.")
				return
			case <-time.After(client.reconnectDelay):
				client.reconnectDelay = client.progressiveDelay(client.reconnectDelay)
			}
			client.logger.Println("continue.")
			continue
		}
		client.logger.Println("set client.reconnectDelay:", client.ReconnectDelay)
		client.reconnectDelay = client.ReconnectDelay
		if done := client.handleReInit(conn); done {
			break
		}
	}
}

// ----------------------------------------------------------------------------

// connect will create a new AMQP connection
func (client *Client) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)

	if err != nil {
		return nil, err
	}

	client.changeConnection(conn)
	client.logger.Println("Connected!")
	return conn, nil
}

// ----------------------------------------------------------------------------

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (client *Client) handleReInit(conn *amqp.Connection) bool {
	for {
		client.isReady = false

		err := client.init(conn)

		if err != nil {
			client.logger.Println("Failed to initialize channel. Retrying in", client.reInitDelay)

			select {
			case <-client.done:
				return true
			case <-time.After(client.reInitDelay):
				client.reInitDelay = client.progressiveDelay(client.reInitDelay)
			}
			continue
		}

		// reset the init delay
		client.reInitDelay = client.ReInitDelay

		select {
		case <-client.done:
			return true
		case <-client.notifyConnClose:
			client.logger.Println("Connection closed. Reconnecting...")
			return false
		case <-client.notifyChanClose:
			client.logger.Println("Channel closed. Re-running init...")
		}
	}
}

// ----------------------------------------------------------------------------

// init will initialize channel, declare the exchange, and declare the queue
func (client *Client) init(conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	err = ch.Confirm(false)

	if err != nil {
		return err
	}
	err = ch.ExchangeDeclare(
		client.ExchangeName, // name
		"direct",            // type
		false,               // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		nil,                 // arguments
	)
	if err != nil {
		return err
	}

	var q amqp.Queue
	q, err = ch.QueueDeclare(
		client.QueueName, // name
		false,            // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		return err
	}

	err = ch.QueueBind(
		q.Name,              // queue name
		client.RoutingKey,   // routing key
		client.ExchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}

	client.changeChannel(ch)
	client.isReady = true
	client.notifyReady <- struct{}{}
	client.logger.Println("Setup!")

	return nil
}

// ----------------------------------------------------------------------------

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (client *Client) changeConnection(connection *amqp.Connection) {
	client.connection = connection
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.connection.NotifyClose(client.notifyConnClose)
}

// ----------------------------------------------------------------------------

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (client *Client) changeChannel(channel *amqp.Channel) {
	client.channel = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.channel.NotifyClose(client.notifyChanClose)
	client.channel.NotifyPublish(client.notifyConfirm)
}

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
func (client *Client) Push(record Record) error {

	if !client.isReady {
		// wait for client to be ready
		<-client.notifyReady
	}
	for {
		err := client.UnsafePush(record)
		if err != nil {
			client.logger.Println("Push failed. Retrying in", client.resendDelay, ". MessageId:", record.GetMessageId()) //TODO:  debug or trace logging, add messageId
			select {
			case <-client.done:
				return errShutdown //TODO:  error message to include messageId?
			case <-time.After(client.resendDelay):
				client.resendDelay = client.progressiveDelay(client.resendDelay)
			}
			continue
		}
		select {
		case confirm := <-client.notifyConfirm:
			if confirm.Ack {
				// reset resend delay
				client.resendDelay = client.ResendDelay
				return nil
			}
		case <-time.After(client.resendDelay):
			client.resendDelay = client.progressiveDelay(client.resendDelay)
		}
		client.logger.Println("Push didn't confirm. Retrying in", client.resendDelay, ". MessageId:", record.GetMessageId()) //TODO:  debug or trace logging, add messageId
	}
}

// ----------------------------------------------------------------------------

// UnsafePush will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for if the server will
// receive the message.
func (client *Client) UnsafePush(record Record) error {

	if !client.isReady {
		// wait for client to be ready
		<-client.notifyReady
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return client.channel.PublishWithContext(
		ctx,                 // context
		client.ExchangeName, // exchange name
		client.RoutingKey,   // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{ // message
			Body:         []byte(record.GetMessage()),
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			MessageId:    record.GetMessageId(),
		},
	)
}

// ----------------------------------------------------------------------------

// Consume will continuously put queue items on the channel.
// It is required to call delivery.Ack when it has been
// successfully processed, or delivery.Nack when it fails.
// Ignoring this will cause data to build up on the server.
//   - prefetch is the number of deliveries for the rabbit mq client to prefetch
//     from the server.  a good rule of thumb is to prefetch as many as there
//     will be clients processing the deliveries.
func (client *Client) Consume(prefetch int) (<-chan amqp.Delivery, error) {
	if !client.isReady {
		// wait for client to be ready
		<-client.notifyReady
	}

	if prefetch < 0 {
		prefetch = 1
	}

	if err := client.channel.Qos(
		prefetch, // prefetch count (should set to the number of load goroutines?)
		0,        // prefetch size
		false,    // global
	); err != nil {
		return nil, err
	}

	return client.channel.Consume(
		client.QueueName, // queue
		"",               // consumer uid, blank auto-generates a uid
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local (not supported by rabbitmq)
		false,            // no-wait, false seems safest
		nil,              // args
	)
}

// ----------------------------------------------------------------------------

// Close will cleanly shutdown the channel and connection.
func (client *Client) Close() error {
	if !client.isReady {
		return errAlreadyClosed
	}
	close(client.done)
	close(client.notifyReady)

	//FIXME:  connection.Close() closes underlying channels so do we need this?
	err := client.channel.Close()
	if err != nil {
		client.logger.Println("channel close error:", err)
	}

	err = client.connection.Close()
	if err != nil {
		client.logger.Println("connection close error:", err)
	}

	client.isReady = false
	return nil
}
