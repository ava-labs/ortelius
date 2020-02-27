package consumers

// BaseType is a basic interface for consumers
type BaseType interface {
	Initialize([]string, string)
	Close()
	ReadMessage()
}
