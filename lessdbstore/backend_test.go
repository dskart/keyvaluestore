package lessdbstore

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ccbrown/keyvaluestore"
	"github.com/ccbrown/keyvaluestore/keyvaluestoretest"
	"github.com/ccbrown/keyvaluestore/lessdbstore/protos/client"
)

func reset(t *testing.T, c Client) {
	resp, err := c.ClearPartitions(context.Background(), &client.ClearPartitionsRequest{})
	require.NoError(t, err)
	require.Nil(t, resp.GetError())
}

func newTestClient(t *testing.T) (Client, error) {
	addr := os.Getenv("LESSDB_ADDRESS")
	if addr == "" {
		return nil, nil
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})
	client := NewClient(conn)
	reset(t, client)
	return client, nil
}

func TestBackend(t *testing.T) {
	client, err := newTestClient(t)
	if err != nil {
		t.Fatal(err)
	} else if client == nil {
		t.Skip("no lessdb server available")
	}
	keyvaluestoretest.TestBackend(t, func() keyvaluestore.Backend {
		reset(t, client)
		return &Backend{
			Client: client,
		}
	})
}
