package memcached

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGet(t *testing.T) {
	if _, err := cp.Set(Item{Key: "Get_1", Value: []byte(`{"get": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	test := func(key string, eis []Item) {
		is, err := cp.Get(key)
		if err != nil {
			t.Fatalf("Failed Get: %+v", err)
		}
		assert.Equal(t, eis, is)
	}
	test("Get_1", []Item{{Key: "Get_1", Value: []byte(`{"get": 1}`)}})
	test("Get_2", []Item{})
}

func TestSet(t *testing.T) {
	test := func(item Item) {
		b, err := cp.Set(item)
		if err != nil {
			t.Fatalf("Failed Set: %+v", err)
		}
		assert.Equal(t, true, b)
	}
	test(Item{Key: "set_1", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1})
	test(Item{Key: "set_2", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1})
	test(Item{Key: "set_3", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1})
	test(Item{Key: "set_5", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1})
}
