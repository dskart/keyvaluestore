package dynamodbstore

import (
	"encoding/binary"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/ccbrown/keyvaluestore"
)

type batchedRead struct {
	key  map[string]*dynamodb.AttributeValue
	item map[string]*dynamodb.AttributeValue
	err  error
}

type getResult struct {
	read *batchedRead
}

func (r getResult) Result() (*string, error) {
	if r.read.item == nil || r.read.err != nil {
		return nil, r.read.err
	}
	return attributeStringValue(r.read.item["v"]), nil
}

type sMembersResult struct {
	read *batchedRead
}

func (r sMembersResult) Result() ([]string, error) {
	if r.read.item == nil || r.read.err != nil {
		return nil, r.read.err
	}
	return attributeStringSliceValue(r.read.item["v"]), nil
}

type zScoreResult struct {
	read *batchedRead
}

func (r zScoreResult) Result() (*float64, error) {
	if r.read.item == nil || r.read.err != nil {
		return nil, r.read.err
	}
	if rk2 := attributeStringValue(r.read.item["rk2"]); rk2 != nil {
		score := sortKeyFloat(*rk2)
		return &score, nil
	}
	return nil, nil
}

type batchedWrite struct {
	request *dynamodb.WriteRequest
	err     error
}

func (w batchedWrite) Result() error {
	return w.err
}

type BatchOperation struct {
	*keyvaluestore.FallbackBatchOperation
	Backend *Backend

	reads  map[string]*batchedRead
	writes map[string]*batchedWrite
}

func combineKeys(hashKey, rangeKey string) string {
	var encodedHashKeyLength [8]byte
	binary.BigEndian.PutUint64(encodedHashKeyLength[:], uint64(len(hashKey)))
	return string(encodedHashKeyLength[:]) + hashKey + rangeKey
}

func (op *BatchOperation) batchRead(hashKey, rangeKey string) *batchedRead {
	if op.reads == nil {
		op.reads = make(map[string]*batchedRead)
	}

	mapKey := combineKeys(hashKey, rangeKey)
	if read, ok := op.reads[mapKey]; ok {
		return read
	}
	read := &batchedRead{
		key: compositeKey(hashKey, rangeKey),
	}
	op.reads[mapKey] = read
	return read
}

func (op *BatchOperation) Get(key string) keyvaluestore.GetResult {
	return getResult{
		read: op.batchRead(key, "_"),
	}
}

func (op *BatchOperation) SMembers(key string) keyvaluestore.SMembersResult {
	return sMembersResult{
		read: op.batchRead(key, "_"),
	}
}

func (op *BatchOperation) ZScore(key string, member interface{}) keyvaluestore.ZScoreResult {
	return zScoreResult{
		read: op.batchRead(key, *keyvaluestore.ToString(member)),
	}
}

func (op *BatchOperation) batchWrite(hashKey, rangeKey string, request *dynamodb.WriteRequest) keyvaluestore.ErrorResult {
	if op.writes == nil {
		op.writes = make(map[string]*batchedWrite)
	}

	mapKey := combineKeys(hashKey, rangeKey)
	if write, ok := op.writes[mapKey]; ok {
		write.request = request
		return write
	}
	write := &batchedWrite{
		request: request,
	}
	op.writes[mapKey] = write
	return write
}

func (op *BatchOperation) Set(key string, value interface{}) keyvaluestore.ErrorResult {
	return op.batchWrite(key, "_", &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: newItem(key, "_", map[string]*dynamodb.AttributeValue{
				"v": attributeValue(value),
			}),
		},
	})
}

func (op *BatchOperation) Delete(key string) keyvaluestore.ErrorResult {
	return op.batchWrite(key, "_", &dynamodb.WriteRequest{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: compositeKey(key, "_"),
		},
	})
}

func (op *BatchOperation) ZAdd(key string, member interface{}, score float64) keyvaluestore.ErrorResult {
	s := *keyvaluestore.ToString(member)
	return op.batchWrite(key, s, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: newItem(key, s, map[string]*dynamodb.AttributeValue{
				"v":   attributeValue(s),
				"rk2": attributeValue(floatSortKey(score) + s),
			}),
		},
	})
}

func (op *BatchOperation) execReads() error {
	keys := make([]map[string]*dynamodb.AttributeValue, len(op.reads))
	i := 0
	for _, read := range op.reads {
		keys[i] = read.key
		i++
	}

	if len(keys) == 0 {
		return nil
	}

	var g errgroup.Group

	for len(keys) > 0 {
		batch := keys
		const maxBatchSize = 100
		if len(batch) > maxBatchSize {
			batch = keys[:maxBatchSize]
		}
		keys = keys[len(batch):]

		g.Go(func() error {
			unprocessed := map[string]*dynamodb.KeysAndAttributes{
				op.Backend.TableName: &dynamodb.KeysAndAttributes{
					ConsistentRead: aws.Bool(!op.Backend.AllowEventuallyConsistentReads),
					Keys:           batch,
				},
			}

			var ret error

			for len(unprocessed) > 0 {
				result, err := op.Backend.Client.BatchGetItem(&dynamodb.BatchGetItemInput{
					RequestItems: unprocessed,
				})
				if err != nil {
					for _, key := range batch {
						mapKey := combineKeys(*attributeStringValue(key["hk"]), *attributeStringValue(key["rk"]))
						if read, ok := op.reads[mapKey]; ok {
							read.err = err
						}
					}
					return errors.Wrap(err, "dynamodb batch get item request error")
				}

				for _, item := range result.Responses[op.Backend.TableName] {
					mapKey := combineKeys(*attributeStringValue(item["hk"]), *attributeStringValue(item["rk"]))
					if read, ok := op.reads[mapKey]; ok {
						read.item = item
					}
				}

				unprocessed = result.UnprocessedKeys
			}

			return ret
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (op *BatchOperation) execWrites() error {
	remainingWrites := make([]*batchedWrite, len(op.writes))
	i := 0
	for _, w := range op.writes {
		remainingWrites[i] = w
		i++
	}

	for len(remainingWrites) > 0 {
		batch := remainingWrites
		const maxBatchSize = 25
		if len(batch) > maxBatchSize {
			batch = remainingWrites[:maxBatchSize]
		}

		writeRequests := make([]*dynamodb.WriteRequest, len(batch))
		for i, w := range batch {
			writeRequests[i] = w.request
		}
		unprocessed := map[string][]*dynamodb.WriteRequest{
			op.Backend.TableName: writeRequests,
		}

		for len(unprocessed) > 0 {
			result, err := op.Backend.Client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
				RequestItems: unprocessed,
			})
			if err != nil {
				for _, w := range remainingWrites {
					w.err = err
				}
				return errors.Wrap(err, "dynamodb batch write item request error")
			}
			unprocessed = result.UnprocessedItems
		}

		remainingWrites = remainingWrites[len(batch):]
	}

	return nil
}

func (op *BatchOperation) Exec() error {
	if err := op.execReads(); err != nil {
		return err
	} else if err := op.execWrites(); err != nil {
		return err
	}
	return op.FallbackBatchOperation.Exec()
}
