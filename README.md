# keyvaluestore [![Documentation](https://godoc.org/github.com/ccbrown/keyvaluestore?status.svg)](https://godoc.org/github.com/ccbrown/keyvaluestore)

This package provides an interface that can be used to build robust applications on top of key-value stores. It supports profiling, caching, batching, eventual consistency, and transactions. It has implementations for Redis, which is ideal for dev environments, DynamoDB, which is ideal in production, and an in-memory store, which is ideal for tests.

This project originated at the AAF, where it was used in production until the company went bankrupt. I (@ccbrown) believe it's the best way to use DynamoDB in Go applications, so I'm continuing to maintain it and use it for other projects.

## Backends

### Memory

In-memory backends are ideal for unit tests as you can create and destroy thousands of them quickly and cheaply:

```go
backend := memorystore.NewBackend()
backend.Set("foo", "bar")
```

### Redis

Redis backends are ideal for dev environments as they're lightweight and easy to spin up and tear down:

```go
backend := &redisstore.Backend{
    Client: redis.NewClient(&redis.Options{
        Addr: "127.0.0.1:6379",
    }),
}
backend.Set("foo", "bar")
```

### DynamoDB

DynamoDB is ideal for production in AWS as it's easy to set up and maintain and scales incredibly well.

When you create your DynamoDB table, you'll need to give it the following schema:

```yaml
DynamoDBTable:
  Type: AWS::DynamoDB::Table
  Properties:
    AttributeDefinitions:
      - AttributeName: hk
        AttributeType: B
      - AttributeName: rk
        AttributeType: B
      - AttributeName: rk2
        AttributeType: B
    KeySchema:
      - AttributeName: hk
        KeyType: HASH
      - AttributeName: rk
        KeyType: RANGE
    LocalSecondaryIndexes:
      - IndexName: rk2
        KeySchema:
          - AttributeName: hk
            KeyType: HASH
          - AttributeName: rk2
            KeyType: RANGE
        Projection:
          ProjectionType: ALL
    BillingMode: PAY_PER_REQUEST
```

If you're using CloudFormation, you can just copy/paste that into your template.

Then you can connect to it like so:

```go
awsConfig := defaults.Get().Config.WithMaxRetries(5)
session := session.Must(session.NewSession(awsConfig))
backend := &KeyValueStore{
    backend: &dynamodbstore.Backend{
        Client: &dynamodbstore.AWSBackendClient{
            DynamoDBAPI: dynamodb.New(session),
        },
        TableName: cfg.TableName,
    },
}
backend.Set("foo", "bar")
```
