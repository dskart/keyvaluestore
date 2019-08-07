package redisstore

import (
	"github.com/go-redis/redis"

	"github.com/ccbrown/keyvaluestore"
)

type BatchOperation struct {
	pipe redis.Pipeliner
}

type GetResult struct {
	*redis.StringCmd
}

func (r *GetResult) Result() (*string, error) {
	v, err := r.StringCmd.Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &v, nil
}

type SMembersResult struct {
	*redis.StringSliceCmd
}

func (r *SMembersResult) Result() ([]string, error) {
	v, err := r.StringSliceCmd.Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return v, nil
}

type RedisCmd interface {
	Err() error
}

type ErrorResult struct {
	RedisCmd
}

func (r *ErrorResult) Result() error {
	return r.RedisCmd.Err()
}

func (op *BatchOperation) Get(key string) keyvaluestore.GetResult {
	return &GetResult{
		op.pipe.Get(key),
	}
}

func (op *BatchOperation) Set(key string, value interface{}) keyvaluestore.ErrorResult {
	return &ErrorResult{
		op.pipe.Set(key, value, 0),
	}
}

func (op *BatchOperation) Delete(key string) keyvaluestore.ErrorResult {
	return &ErrorResult{
		op.pipe.Del(key),
	}
}

func (op *BatchOperation) SMembers(key string) keyvaluestore.SMembersResult {
	return &SMembersResult{
		op.pipe.SMembers(key),
	}
}

func (op *BatchOperation) SAdd(key string, member interface{}, members ...interface{}) keyvaluestore.ErrorResult {
	return &ErrorResult{
		op.pipe.SAdd(key, append([]interface{}{member}, members...)...),
	}
}

func (op *BatchOperation) SRem(key string, member interface{}, members ...interface{}) keyvaluestore.ErrorResult {
	return &ErrorResult{
		op.pipe.SRem(key, append([]interface{}{member}, members...)...),
	}
}

func (op *BatchOperation) ZAdd(key string, member interface{}, score float64) keyvaluestore.ErrorResult {
	return &ErrorResult{
		op.pipe.ZAdd(key, redis.Z{
			Member: member,
			Score:  score,
		}),
	}
}

func (op *BatchOperation) ZRem(key string, member interface{}) keyvaluestore.ErrorResult {
	return &ErrorResult{
		op.pipe.ZRem(key, member),
	}
}

type ZScoreResult struct {
	*redis.FloatCmd
}

func (r *ZScoreResult) Result() (*float64, error) {
	v, err := r.FloatCmd.Result()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &v, nil
}

func (op *BatchOperation) ZScore(key string, member interface{}) keyvaluestore.ZScoreResult {
	return &ZScoreResult{
		op.pipe.ZScore(key, *keyvaluestore.ToString(member)),
	}
}

func (op *BatchOperation) Exec() error {
	cmds, _ := op.pipe.Exec()
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil && err != redis.Nil {
			return err
		}
	}
	return nil
}
