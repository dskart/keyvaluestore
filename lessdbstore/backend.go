package lessdbstore

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"google.golang.org/grpc"

	"github.com/ccbrown/keyvaluestore"
	"github.com/ccbrown/keyvaluestore/lessdbstore/protos/client"
)

var NotSupportedErr = fmt.Errorf("not supported")

type Client client.ServiceClient

func NewClient(cc grpc.ClientConnInterface) Client {
	return client.NewServiceClient(cc)
}

type Backend struct {
	Client Client
}

func (b *Backend) Batch() keyvaluestore.BatchOperation {
	return &keyvaluestore.FallbackBatchOperation{
		Backend: b,
	}
}

func (b *Backend) AtomicWrite() keyvaluestore.AtomicWriteOperation {
	return &AtomicWriteOperation{Backend: b}
}

type Error struct {
	err *client.Error
}

func newError(err *client.Error) Error {
	return Error{
		err: err,
	}
}

func (e Error) Error() string {
	return e.err.Message
}

func (b *Backend) Delete(key string) (bool, error) {
	if resp, err := b.Client.Delete(context.Background(), &client.DeleteRequest{
		Key: toHash(key),
	}); err != nil {
		return false, err
	} else if err := resp.GetError(); err != nil {
		return false, newError(err)
	} else {
		return resp.GetResult().DidDelete, nil
	}
}

func valueToString(v *client.Value) *string {
	if v == nil {
		return nil
	}
	switch v := v.Value.(type) {
	case *client.Value_Bytes:
		s := string(v.Bytes)
		return &s
	case *client.Value_Int:
		s := strconv.FormatInt(v.Int, 10)
		return &s
	}
	return nil
}

func arrayValueToStrings(v *client.Value) []string {
	if v == nil {
		return nil
	}
	switch v := v.Value.(type) {
	case *client.Value_Array:
		if v.Array != nil {
			ret := make([]string, len(v.Array.Values))
			for i, v := range v.Array.Values {
				ret[i] = *valueToString(v)
			}
			return ret
		}
	}
	return nil
}

func valuesToStrings(v []*client.Value) []string {
	ret := make([]string, 0, len(v))
	for _, v := range v {
		if s := valueToString(v); s != nil {
			ret = append(ret, *s)
		}
	}
	return ret
}

func toHash(key string) []byte {
	h := sha256.New()
	h.Write([]byte(key))
	return h.Sum(nil)
}

func toScalar(v interface{}) *client.Scalar {
	switch v := v.(type) {
	case []byte:
		return &client.Scalar{
			Value: &client.Scalar_Bytes{
				Bytes: v,
			},
		}
	case string:
		return toScalar([]byte(v))
	case int:
		return toScalar(int64(v))
	case int64:
		return &client.Scalar{
			Value: &client.Scalar_Int{
				Int: v,
			},
		}
	case *client.Scalar:
		return v
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			panic("binary marshaler values shouldn't panic. error: " + err.Error())
		}
		return toScalar(b)
	}
	panic(fmt.Sprintf("unsupported scalar type: %T", v))
}

func toValue(v interface{}) *client.Value {
	switch v := v.(type) {
	case []byte:
		return &client.Value{
			Value: &client.Value_Bytes{
				Bytes: v,
			},
		}
	case string:
		return toValue([]byte(v))
	case int:
		return toValue(int64(v))
	case int64:
		return &client.Value{
			Value: &client.Value_Int{
				Int: v,
			},
		}
	case *client.Scalar:
		switch v := v.Value.(type) {
		case *client.Scalar_Bytes:
			return toValue(v.Bytes)
		case *client.Scalar_Int:
			return toValue(v.Int)
		}
	case *client.Value:
		return v
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			panic("binary marshaler values shouldn't panic. error: " + err.Error())
		}
		return toValue(b)
	}
	panic(fmt.Sprintf("unsupported value type: %T", v))
}

func (b *Backend) Get(key string) (*string, error) {
	if resp, err := b.Client.Get(context.Background(), &client.GetRequest{
		Key: toHash(key),
	}); err != nil {
		return nil, err
	} else if err := resp.GetError(); err != nil {
		return nil, newError(err)
	} else {
		return valueToString(resp.GetResult().Value), nil
	}
}

func (b *Backend) Set(key string, value interface{}) error {
	if resp, err := b.Client.Set(context.Background(), &client.SetRequest{
		Key:   toHash(key),
		Value: toValue(value),
	}); err != nil {
		return err
	} else if err := resp.GetError(); err != nil {
		return newError(err)
	} else {
		return nil
	}
}

func (b *Backend) IncrBy(key string, n int64) (int64, error) {
	return 0, NotSupportedErr
}

func (b *Backend) ZIncrBy(key string, member string, n float64) (float64, error) {
	return 0.0, NotSupportedErr
}

func (b *Backend) SAdd(key string, member interface{}, members ...interface{}) error {
	values := make([]*client.Value, 1+len(members))
	values[0] = toValue(member)
	for i, m := range members {
		values[i+1] = toValue(m)
	}
	if resp, err := b.Client.SetAdd(context.Background(), &client.SetAddRequest{
		Key:     toHash(key),
		Members: values,
	}); err != nil {
		return err
	} else if err := resp.GetError(); err != nil {
		return newError(err)
	} else {
		return nil
	}
}

func (b *Backend) SRem(key string, member interface{}, members ...interface{}) error {
	values := make([]*client.Value, 1+len(members))
	values[0] = toValue(member)
	for i, m := range members {
		values[i+1] = toValue(m)
	}
	if resp, err := b.Client.SetRemove(context.Background(), &client.SetRemoveRequest{
		Key:     toHash(key),
		Members: values,
	}); err != nil {
		return err
	} else if err := resp.GetError(); err != nil {
		return newError(err)
	} else {
		return nil
	}
}

func (b *Backend) SMembers(key string) ([]string, error) {
	if resp, err := b.Client.Get(context.Background(), &client.GetRequest{
		Key: toHash(key),
	}); err != nil {
		return nil, err
	} else if err := resp.GetError(); err != nil {
		return nil, newError(err)
	} else {
		return arrayValueToStrings(resp.GetResult().Value), nil
	}
}

func (b *Backend) HSet(key, field string, value interface{}, fields ...keyvaluestore.KeyValue) error {
	return NotSupportedErr
}

func (b *Backend) HDel(key string, field string, fields ...string) error {
	return NotSupportedErr
}

func (b *Backend) HGet(key, field string) (*string, error) {
	return nil, NotSupportedErr
}

func (b *Backend) HGetAll(key string) (map[string]string, error) {
	return nil, NotSupportedErr
}

func (b *Backend) SetNX(key string, value interface{}) (bool, error) {
	if resp, err := b.Client.Set(context.Background(), &client.SetRequest{
		Key:   toHash(key),
		Value: toValue(value),
		Condition: &client.SetRequest_Condition{
			Value: &client.SetRequest_Condition_Exists{
				Exists: false,
			},
		},
	}); err != nil {
		return false, err
	} else if err := resp.GetError(); err != nil {
		return false, newError(err)
	} else {
		return resp.GetResult().DidSet, nil
	}
}

func (b *Backend) SetXX(key string, value interface{}) (bool, error) {
	if resp, err := b.Client.Set(context.Background(), &client.SetRequest{
		Key:   toHash(key),
		Value: toValue(value),
		Condition: &client.SetRequest_Condition{
			Value: &client.SetRequest_Condition_Exists{
				Exists: true,
			},
		},
	}); err != nil {
		return false, err
	} else if err := resp.GetError(); err != nil {
		return false, newError(err)
	} else {
		return resp.GetResult().DidSet, nil
	}
}

func (b *Backend) SetEQ(key string, value, oldValue interface{}) (bool, error) {
	if resp, err := b.Client.Set(context.Background(), &client.SetRequest{
		Key:   toHash(key),
		Value: toValue(value),
		Condition: &client.SetRequest_Condition{
			Value: &client.SetRequest_Condition_Equals{
				Equals: toValue(oldValue),
			},
		},
	}); err != nil {
		return false, err
	} else if err := resp.GetError(); err != nil {
		return false, newError(err)
	} else {
		return resp.GetResult().DidSet, nil
	}
}

func (b *Backend) ZAdd(key string, member interface{}, score float64) error {
	m := toScalar(member)
	return b.zhAdd(key, m, m, score)
}

func (b *Backend) ZHAdd(key, field string, member interface{}, score float64) error {
	return b.zhAdd(key, field, member, score)
}

const floatSortKeyNumBytes = 8

func floatSortKey(f float64) []byte {
	n := math.Float64bits(f)
	if (n & (1 << 63)) != 0 {
		n ^= 0xffffffffffffffff
	} else {
		n ^= 0x8000000000000000
	}
	buf := make([]byte, floatSortKeyNumBytes)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

func (b *Backend) zhAdd(key string, field interface{}, member interface{}, score float64) error {
	if resp, err := b.Client.MapSet(context.Background(), &client.MapSetRequest{
		Key:   toHash(key),
		Field: toScalar(field),
		Value: toValue(member),
		Order: toScalar(floatSortKey(score)),
	}); err != nil {
		return err
	} else if err := resp.GetError(); err != nil {
		return newError(err)
	} else {
		return nil
	}
}

func (b *Backend) ZScore(key string, member interface{}) (*float64, error) {
	return nil, NotSupportedErr
}

func (b *Backend) ZRem(key string, member interface{}) error {
	return b.zhRem(key, member)
}

func (b *Backend) ZHRem(key, field string) error {
	return b.zhRem(key, field)
}

func (b *Backend) zhRem(key string, field interface{}) error {
	if resp, err := b.Client.MapDelete(context.Background(), &client.MapDeleteRequest{
		Key:   toHash(key),
		Field: toScalar(field),
	}); err != nil {
		return err
	} else if err := resp.GetError(); err != nil {
		return newError(err)
	} else {
		return nil
	}
}

func (b *Backend) ZRangeByScore(key string, min, max float64, limit int) ([]string, error) {
	return b.ZHRangeByScore(key, min, max, limit)
}

func (b *Backend) ZHRangeByScore(key string, min, max float64, limit int) ([]string, error) {
	if resp, err := b.Client.MapGetRange(context.Background(), &client.MapGetRangeRequest{
		Key:   toHash(key),
		Start: scoreBound(min),
		End:   scoreBound(max),
		Limit: uint64(limit),
	}); err != nil {
		return nil, err
	} else if err := resp.GetError(); err != nil {
		return nil, newError(err)
	} else {
		return valuesToStrings(resp.GetResult().Values), nil
	}
}

func (b *Backend) ZRangeByScoreWithScores(key string, min, max float64, limit int) (keyvaluestore.ScoredMembers, error) {
	return nil, NotSupportedErr
}

func (b *Backend) ZHRangeByScoreWithScores(key string, min, max float64, limit int) (keyvaluestore.ScoredMembers, error) {
	return nil, NotSupportedErr
}

func (b *Backend) ZRevRangeByScore(key string, min, max float64, limit int) ([]string, error) {
	return b.ZHRevRangeByScore(key, min, max, limit)
}

func (b *Backend) ZHRevRangeByScore(key string, min, max float64, limit int) ([]string, error) {
	if resp, err := b.Client.MapGetRange(context.Background(), &client.MapGetRangeRequest{
		Key:     toHash(key),
		Start:   scoreBound(min),
		End:     scoreBound(max),
		Limit:   uint64(limit),
		Reverse: true,
	}); err != nil {
		return nil, err
	} else if err := resp.GetError(); err != nil {
		return nil, newError(err)
	} else {
		return valuesToStrings(resp.GetResult().Values), nil
	}
}

func (b *Backend) ZRevRangeByScoreWithScores(key string, min, max float64, limit int) (keyvaluestore.ScoredMembers, error) {
	return nil, NotSupportedErr
}

func (b *Backend) ZHRevRangeByScoreWithScores(key string, min, max float64, limit int) (keyvaluestore.ScoredMembers, error) {
	return nil, NotSupportedErr
}

func scoreBound(f float64) *client.Bound {
	if !math.IsInf(f, 0) {
		return &client.Bound{
			Value: &client.Bound_Included{
				Included: toScalar(floatSortKey(f)),
			},
		}
	}
	return nil
}

func (b *Backend) ZCount(key string, min, max float64) (int, error) {
	if resp, err := b.Client.MapCountRange(context.Background(), &client.MapCountRangeRequest{
		Key:   toHash(key),
		Start: scoreBound(min),
		End:   scoreBound(max),
	}); err != nil {
		return 0, err
	} else if err := resp.GetError(); err != nil {
		return 0, newError(err)
	} else {
		return int(resp.GetResult().Count), nil
	}
}

func lexBound(s string) *client.Bound {
	if s == "-" || s == "+" {
		return nil
	} else if s[0] == '(' {
		return &client.Bound{
			Value: &client.Bound_Excluded{
				Excluded: toScalar(s[1:]),
			},
		}
	} else {
		return &client.Bound{
			Value: &client.Bound_Included{
				Included: toScalar(s[1:]),
			},
		}
	}
}

func (b *Backend) ZLexCount(key string, min, max string) (int, error) {
	if resp, err := b.Client.MapCountRangeByField(context.Background(), &client.MapCountRangeByFieldRequest{
		Key:   toHash(key),
		Start: lexBound(min),
		End:   lexBound(max),
	}); err != nil {
		return 0, err
	} else if err := resp.GetError(); err != nil {
		return 0, newError(err)
	} else {
		return int(resp.GetResult().Count), nil
	}
}

func (b *Backend) ZRangeByLex(key string, min, max string, limit int) ([]string, error) {
	return b.ZHRangeByLex(key, min, max, limit)
}

func (b *Backend) ZHRangeByLex(key string, min, max string, limit int) ([]string, error) {
	if resp, err := b.Client.MapGetRangeByField(context.Background(), &client.MapGetRangeByFieldRequest{
		Key:   toHash(key),
		Start: lexBound(min),
		End:   lexBound(max),
		Limit: uint64(limit),
	}); err != nil {
		return nil, err
	} else if err := resp.GetError(); err != nil {
		return nil, newError(err)
	} else {
		return valuesToStrings(resp.GetResult().Values), nil
	}
}

func (b *Backend) ZRevRangeByLex(key string, min, max string, limit int) ([]string, error) {
	return b.ZHRevRangeByLex(key, min, max, limit)
}

func (b *Backend) ZHRevRangeByLex(key string, min, max string, limit int) ([]string, error) {
	if resp, err := b.Client.MapGetRangeByField(context.Background(), &client.MapGetRangeByFieldRequest{
		Key:     toHash(key),
		Start:   lexBound(min),
		End:     lexBound(max),
		Limit:   uint64(limit),
		Reverse: true,
	}); err != nil {
		return nil, err
	} else if err := resp.GetError(); err != nil {
		return nil, newError(err)
	} else {
		return valuesToStrings(resp.GetResult().Values), nil
	}
}

func (b *Backend) WithProfiler(profiler interface{}) keyvaluestore.Backend {
	return b
}

func (b *Backend) WithEventuallyConsistentReads() keyvaluestore.Backend {
	return b
}

func (b *Backend) Unwrap() keyvaluestore.Backend {
	return nil
}
