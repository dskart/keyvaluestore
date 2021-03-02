package cassandrastore

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/gocql/gocql"

	"github.com/ccbrown/keyvaluestore"
)

var ErrTODO = fmt.Errorf("TODO")

type Backend struct {
	Session *gocql.Session

	useEventuallyConsistentReads bool
}

func (b *Backend) WithProfiler(profiler interface{}) keyvaluestore.Backend {
	return b
}

func (b *Backend) WithEventuallyConsistentReads() keyvaluestore.Backend {
	if b.useEventuallyConsistentReads {
		return b
	}
	ret := *b
	ret.useEventuallyConsistentReads = true
	return &ret
}

func (b *Backend) Batch() keyvaluestore.BatchOperation {
	return &keyvaluestore.FallbackBatchOperation{
		Backend: b,
	}
}

func toBytes(v interface{}) []byte {
	switch v := v.(type) {
	case []byte:
		return v
	case string:
		return []byte(v)
	case int:
		return toBytes(int64(v))
	case int64:
		return []byte(strconv.FormatInt(v, 10))
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			panic("binary marshaler values shouldn't panic. error: " + err.Error())
		}
		return b
	}
	panic(fmt.Sprintf("unsupported value type: %T", v))
}

func (b *Backend) NIncrBy(key string, n int64) (int64, error) {
	return 0, ErrTODO
}

func (b *Backend) Delete(key string) (bool, error) {
	m := map[string]interface{}{}
	return b.Session.Query(`DELETE FROM kvs WHERE hk = ? AND rk = ? AND rk2 = ? IF EXISTS`, []byte(key), empty, empty).MapScanCAS(m)
}

func (b *Backend) consistency() gocql.Consistency {
	if b.useEventuallyConsistentReads {
		return gocql.One
	}
	return gocql.LocalQuorum
}

var empty = []byte{}

func (b *Backend) Get(key string) (*string, error) {
	iter := b.Session.Query(`SELECT b FROM kvs WHERE hk = ? AND rk = ? AND rk2 = ?`, []byte(key), empty, empty).Consistency(b.consistency()).Iter()
	var buf []byte
	if iter.Scan(&buf) {
		s := string(buf)
		return &s, nil
	}
	return nil, iter.Close()
}

func (b *Backend) Set(key string, value interface{}) error {
	return b.Session.Query(`INSERT INTO kvs (hk, rk, rk2, b) VALUES (?, ?, ?, ?)`, []byte(key), empty, empty, toBytes(value)).Exec()
}

func (b *Backend) SetNX(key string, value interface{}) (bool, error) {
	m := map[string]interface{}{}
	return b.Session.Query(`INSERT INTO kvs (hk, rk, rk2, b) VALUES (?, ?, ?, ?) IF NOT EXISTS`, []byte(key), empty, empty, toBytes(value)).MapScanCAS(m)
}

func (b *Backend) SetXX(key string, value interface{}) (bool, error) {
	m := map[string]interface{}{}
	return b.Session.Query(`UPDATE kvs SET b = ? WHERE hk = ? AND rk = ? AND rk2 = ? IF EXISTS`, toBytes(value), []byte(key), empty, empty).MapScanCAS(m)
}

func (b *Backend) SetEQ(key string, value, oldValue interface{}) (bool, error) {
	m := map[string]interface{}{}
	return b.Session.Query(`UPDATE kvs SET b = ? WHERE hk = ? AND rk = ? AND rk2 = ? IF b = ?`, toBytes(value), []byte(key), empty, empty, toBytes(oldValue)).MapScanCAS(m)
}

func (b *Backend) SAdd(key string, member interface{}, members ...interface{}) error {
	return ErrTODO
}

func (b *Backend) SRem(key string, member interface{}, members ...interface{}) error {
	return ErrTODO
}

func (b *Backend) SMembers(key string) ([]string, error) {
	return nil, ErrTODO
}

func (b *Backend) HSet(key, field string, value interface{}, fields ...keyvaluestore.KeyValue) error {
	return ErrTODO
}

func (b *Backend) HDel(key, field string, fields ...string) error {
	return ErrTODO
}

func (b *Backend) HGet(key, field string) (*string, error) {
	return nil, ErrTODO
}

func (b *Backend) HGetAll(key string) (map[string]string, error) {
	return nil, ErrTODO
}

func (b *Backend) ZAdd(key string, member interface{}, score float64) error {
	s := *keyvaluestore.ToString(member)
	return b.ZHAdd(key, s, s, score)
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

func (b *Backend) ZHAdd(key, field string, member interface{}, score float64) error {
	prevScore, err := b.ZScore(key, field)
	if err != nil {
		return err
	}
	batch := b.Session.NewBatch(gocql.UnloggedBatch)
	if prevScore == nil {
		batch.Query(`INSERT INTO kvs (hk, rk, rk2, d) VALUES (?, ?, ?, ?) IF NOT EXISTS`, []byte(key), empty, []byte(field), score)
		batch.Query(`INSERT INTO kvs (hk, rk, rk2, b, d) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`, []byte(key), floatSortKey(score), []byte(field), toBytes(member), score)
	} else {
		batch.Query(`UPDATE kvs SET d = ? WHERE hk = ? AND rk = ? AND rk2 = ? IF d = ?`, score, []byte(key), empty, []byte(field), *prevScore)
		if *prevScore == score {
			batch.Query(`UPDATE kvs SET b = ?, d = ? WHERE hk = ? AND rk = ? AND rk2 = ? IF d = ?`, toBytes(member), score, []byte(key), floatSortKey(score), []byte(field), *prevScore)
		} else {
			batch.Query(`DELETE FROM kvs WHERE hk = ? AND rk = ? AND rk2 = ? IF d = ?`, []byte(key), floatSortKey(*prevScore), []byte(field), *prevScore)
			batch.Query(`INSERT INTO kvs (hk, rk, rk2, b, d) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`, []byte(key), floatSortKey(score), []byte(field), toBytes(member), score)
		}
	}
	return b.Session.ExecuteBatch(batch)
}

func (b *Backend) ZScore(key string, member interface{}) (*float64, error) {
	s := *keyvaluestore.ToString(member)
	iter := b.Session.Query(`SELECT d FROM kvs WHERE hk = ? AND rk = ? AND rk2 = ?`, []byte(key), empty, []byte(s)).Iter()
	var ret float64
	if iter.Scan(&ret) {
		return &ret, nil
	}
	return nil, iter.Close()
}

func (b *Backend) ZRem(key string, member interface{}) error {
	s := *keyvaluestore.ToString(member)
	return b.ZHRem(key, s)
}

func (b *Backend) ZHRem(key, field string) error {
	score, err := b.ZScore(key, field)
	if score == nil || err != nil {
		return err
	}
	batch := b.Session.NewBatch(gocql.UnloggedBatch)
	batch.Query(`DELETE FROM kvs WHERE hk = ? AND rk = ? AND rk2 = ? IF d = ?`, []byte(key), empty, []byte(field), *score)
	batch.Query(`DELETE FROM kvs WHERE hk = ? AND rk = ? AND rk2 = ? IF d = ?`, []byte(key), floatSortKey(*score), []byte(field), *score)
	return b.Session.ExecuteBatch(batch)
}

func scoreCondition(key string, min, max float64) (string, []interface{}) {
	ret := "hk = ?"
	vars := []interface{}{
		[]byte(key),
	}
	if min == math.Inf(-1) {
		ret += " AND rk > ?"
		vars = append(vars, empty)
	} else {
		ret += " AND rk >= ?"
		vars = append(vars, floatSortKey(min))
	}
	if max != math.Inf(1) {
		ret += " AND rk <= ?"
		vars = append(vars, floatSortKey(max))
	}
	return ret, vars
}

func (b *Backend) ZCount(key string, min, max float64) (int, error) {
	cond, vars := scoreCondition(key, min, max)
	iter := b.Session.Query("SELECT COUNT(*) FROM kvs WHERE "+cond, vars...).Iter()
	var ret int
	if iter.Scan(&ret) {
		return ret, nil
	}
	return 0, iter.Close()
}

func (b *Backend) ZLexCount(key, min, max string) (int, error) {
	cond, vars := lexCondition(key, min, max)
	iter := b.Session.Query("SELECT COUNT(*) FROM kvs WHERE "+cond, vars...).Iter()
	var ret int
	if iter.Scan(&ret) {
		return ret, nil
	}
	return 0, iter.Close()
}

func (b *Backend) ZRangeByScore(key string, min, max float64, limit int) ([]string, error) {
	members, err := b.ZRangeByScoreWithScores(key, min, max, limit)
	return members.Values(), err
}

func (b *Backend) ZHRangeByScore(key string, min, max float64, limit int) ([]string, error) {
	return b.ZRangeByScore(key, min, max, limit)
}

func (b *Backend) ZRangeByScoreWithScores(key string, min, max float64, limit int) (keyvaluestore.ScoredMembers, error) {
	return b.zRangeByScoreWithScores(key, min, max, limit, false)
}

func (b *Backend) zRangeByScoreWithScores(key string, min, max float64, limit int, reverse bool) (keyvaluestore.ScoredMembers, error) {
	cond, vars := scoreCondition(key, min, max)
	q := "SELECT b, d FROM kvs WHERE " + cond
	if reverse {
		q += " ORDER BY rk DESC"
	}
	if limit != 0 {
		q += " LIMIT " + strconv.Itoa(limit)
	}
	iter := b.Session.Query(q, vars...).Iter()
	var ret keyvaluestore.ScoredMembers
	var buf []byte
	var score float64
	for iter.Scan(&buf, &score) {
		ret = append(ret, &keyvaluestore.ScoredMember{
			Value: string(buf),
			Score: score,
		})
	}
	return ret, iter.Close()
}

func (b *Backend) ZHRangeByScoreWithScores(key string, min, max float64, limit int) (keyvaluestore.ScoredMembers, error) {
	return b.ZRangeByScoreWithScores(key, min, max, limit)
}

func (b *Backend) ZRevRangeByScore(key string, min, max float64, limit int) ([]string, error) {
	members, err := b.ZRevRangeByScoreWithScores(key, min, max, limit)
	return members.Values(), err
}

func (b *Backend) ZHRevRangeByScore(key string, min, max float64, limit int) ([]string, error) {
	return b.ZRevRangeByScore(key, min, max, limit)
}

func (b *Backend) ZRevRangeByScoreWithScores(key string, min, max float64, limit int) (keyvaluestore.ScoredMembers, error) {
	return b.zRangeByScoreWithScores(key, min, max, limit, true)
}

func (b *Backend) ZHRevRangeByScoreWithScores(key string, min, max float64, limit int) (keyvaluestore.ScoredMembers, error) {
	return b.ZRevRangeByScoreWithScores(key, min, max, limit)
}

func (b *Backend) ZRangeByLex(key string, min, max string, limit int) ([]string, error) {
	return b.ZHRangeByLex(key, min, max, limit)
}

func (b *Backend) ZHRangeByLex(key string, min, max string, limit int) ([]string, error) {
	return b.zHRangeByLex(key, min, max, limit, false)
}

func lexCondition(key, min, max string) (string, []interface{}) {
	ret := "hk = ? AND rk = ?"
	vars := []interface{}{
		[]byte(key),
		floatSortKey(0.0),
	}
	if min[0] == '[' {
		ret += " AND rk2 >= ?"
		vars = append(vars, []byte(min[1:]))
	} else if min[0] == '(' {
		ret += " AND rk2 > ?"
		vars = append(vars, []byte(min[1:]))
	}
	if max[0] == '[' {
		ret += " AND rk2 <= ?"
		vars = append(vars, []byte(max[1:]))
	} else if max[0] == '(' {
		ret += " AND rk2 < ?"
		vars = append(vars, []byte(max[1:]))
	}
	return ret, vars
}

func (b *Backend) zHRangeByLex(key, min, max string, limit int, reverse bool) ([]string, error) {
	cond, vars := lexCondition(key, min, max)
	q := "SELECT b FROM kvs WHERE " + cond
	if reverse {
		q += " ORDER BY rk DESC"
	}
	if limit != 0 {
		q += " LIMIT " + strconv.Itoa(limit)
	}
	iter := b.Session.Query(q, vars...).Iter()
	var ret []string
	var buf []byte
	for iter.Scan(&buf) {
		ret = append(ret, string(buf))
	}
	return ret, iter.Close()
}

func (b *Backend) ZRevRangeByLex(key string, min, max string, limit int) ([]string, error) {
	return b.ZHRevRangeByLex(key, min, max, limit)
}

func (b *Backend) ZHRevRangeByLex(key string, min, max string, limit int) ([]string, error) {
	return b.zHRangeByLex(key, min, max, limit, true)
}

func (b *Backend) Unwrap() keyvaluestore.Backend {
	return nil
}
