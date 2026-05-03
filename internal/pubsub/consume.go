package pubsub

type SimpleQueueType int

const (
	SimpleQueueDurable   SimpleQueueType = iota // = 0
	SimpleQueueTransient                        // = 1
)
