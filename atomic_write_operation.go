package keyvaluestore

type AtomicWriteResult interface {
	// Returns false if the transaction failed due to this operation's conditional failing.
	ConditionalFailed() bool
}

// DynamoDB can't do more than 25 operations in an atomic write. So all stores should enforce this
// limit.
const MaxAtomicWriteOperations = 25

type AtomicWriteOperation interface {
	Set(key string, value interface{}) AtomicWriteResult
	SetNX(key string, value interface{}) AtomicWriteResult
	CAS(key string, oldValue, newValue string) AtomicWriteResult
	Delete(key string) AtomicWriteResult
	ZAdd(key string, member interface{}, score float64) AtomicWriteResult

	// Executes the operation. If a condition failed, returns false.
	Exec() (bool, error)
}
