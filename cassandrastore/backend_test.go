package cassandrastore

import (
	"os"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/ccbrown/keyvaluestore"
	"github.com/ccbrown/keyvaluestore/keyvaluestoretest"
)

func TestBackend(t *testing.T) {
	keyspace := os.Getenv("CASSANDRA_KEYSPACE")
	if keyspace == "" {
		t.Skip("no cassandra keyspace specified")
	}

	{
		cluster := gocql.NewCluster("127.0.0.1:9042")
		sess, err := cluster.CreateSession()
		require.NoError(t, err)
		defer sess.Close()

		require.NoError(t, sess.Query("CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec())
	}

	cluster := gocql.NewCluster("127.0.0.1:9042")
	cluster.Keyspace = keyspace

	sess, err := cluster.CreateSession()
	require.NoError(t, err)
	defer sess.Close()

	require.NoError(t, sess.Query("CREATE KEYSPACE IF NOT EXISTS "+keyspace+" WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}").Exec())
	sess.Query("DROP TABLE kvs").Exec()
	require.NoError(t, sess.Query("CREATE TABLE kvs (hk blob, rk blob, rk2 blob, b blob, d double, PRIMARY KEY (hk, rk, rk2))").Exec())

	keyvaluestoretest.TestBackend(t, func() keyvaluestore.Backend {
		require.NoError(t, sess.Query("TRUNCATE kvs").Exec())

		return &Backend{
			Session: sess,
		}
	})
}
