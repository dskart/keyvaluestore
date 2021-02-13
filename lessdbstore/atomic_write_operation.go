package lessdbstore

import (
	"github.com/ccbrown/keyvaluestore"
)

type AtomicWriteOperation struct {
	Backend *Backend
	ops     []func() error
}

type atomicWriteResult struct{}

func (r *atomicWriteResult) ConditionalFailed() bool {
	return false
}

func (op *AtomicWriteOperation) Set(key string, value interface{}) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		return op.Backend.Set(key, value)
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) SetNX(key string, value interface{}) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		_, err := op.Backend.SetNX(key, value)
		return err
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) SetXX(key string, value interface{}) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		_, err := op.Backend.SetXX(key, value)
		return err
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) SetEQ(key string, value, oldValue interface{}) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		_, err := op.Backend.SetEQ(key, value, oldValue)
		return err
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) Delete(key string) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		_, err := op.Backend.Delete(key)
		return err
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) DeleteXX(key string) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		_, err := op.Backend.Delete(key)
		return err
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) IncrBy(key string, n int64) keyvaluestore.AtomicWriteResult {
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) ZAdd(key string, member interface{}, score float64) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		return op.Backend.ZAdd(key, member, score)
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) ZHAdd(key, field string, member interface{}, score float64) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		return op.Backend.ZHAdd(key, field, member, score)
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) ZAddNX(key string, member interface{}, score float64) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		return op.Backend.ZAdd(key, member, score)
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) ZRem(key string, member interface{}) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		return op.Backend.ZRem(key, member)
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) ZHRem(key, field string) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		return op.Backend.ZHRem(key, field)
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) SAdd(key string, member interface{}, members ...interface{}) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		return op.Backend.SAdd(key, member, members...)
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) SRem(key string, member interface{}, members ...interface{}) keyvaluestore.AtomicWriteResult {
	op.ops = append(op.ops, func() error {
		return op.Backend.SRem(key, member, members...)
	})
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) HSet(key, field string, value interface{}, fields ...keyvaluestore.KeyValue) keyvaluestore.AtomicWriteResult {
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) HSetNX(key, field string, value interface{}) keyvaluestore.AtomicWriteResult {
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) HDel(key, field string, fields ...string) keyvaluestore.AtomicWriteResult {
	return &atomicWriteResult{}
}

func (op *AtomicWriteOperation) Exec() (bool, error) {
	for _, op := range op.ops {
		if err := op(); err != nil {
			return false, err
		}
	}
	return true, nil
}
