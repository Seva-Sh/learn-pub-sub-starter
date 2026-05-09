package pubsub

type SimpleQueueType int

const (
	SimpleQueueDurable   SimpleQueueType = iota // = 0
	SimpleQueueTransient                        // = 1
)

type Acktype int

const (
	Ack         Acktype = iota // = 0
	NackRequeue                // = 1
	NackDiscard                // = 2
)
