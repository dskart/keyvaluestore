package keyvaluestorecache_test

import (
	"testing"

	"github.com/ccbrown/keyvaluestore"
	"github.com/ccbrown/keyvaluestore/keyvaluestorecache"
	"github.com/ccbrown/keyvaluestore/keyvaluestoretest"
	"github.com/ccbrown/keyvaluestore/memorystore"
)

func TestReadCache(t *testing.T) {
	keyvaluestoretest.TestBackend(t, func() keyvaluestore.Backend {
		return keyvaluestorecache.NewReadCache(memorystore.NewBackend())
	})
}
