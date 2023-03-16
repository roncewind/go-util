package queues

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type Record interface {
	GetMessage() string
	GetMessageId() string
}
