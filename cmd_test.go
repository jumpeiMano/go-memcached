package memcached

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSet(t *testing.T) {
	test := func(key string, value []byte) {
		b, err := cp.Set(key, 0, value)
		if err != nil {
			t.Fatalf("Failed Set: %+v", err)
		}
		assert.Equal(t, true, b)
	}
	test("key1", []byte(`{"id": 1, "test": "ok"}`))
	test("key2", []byte(`{"id": 1, "test": "ok"}`))
	test("key3", []byte(`{"id": 1, "test": "ok"}`))
	test("key6", []byte(`{"id": 1, "test": "ok"}`))
}
