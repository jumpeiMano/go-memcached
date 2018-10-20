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

func TestGets(t *testing.T) {
	if _, err := cp.Set(Item{Key: "Gets_1", Value: []byte(`{"gets": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	test := func(key string, evs [][]byte) {
		is, err := cp.Gets(key)
		if err != nil {
			t.Fatalf("Failed Gets: %+v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test("Gets_1", [][]byte{[]byte(`{"gets": 1}`)})
	test("Gets_2", [][]byte{})
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

func TestAdd(t *testing.T) {
	if _, err := cp.Set(Item{Key: "Add_1", Value: []byte(`{"add": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	test := func(item Item, eBool bool, evs [][]byte) {
		b, err := cp.Add(item)
		if err != nil {
			t.Fatalf("Failed Add: %+v", err)
		}
		assert.Equal(t, eBool, b)
		is, err := cp.Get(item.Key)
		if err != nil {
			t.Fatalf("Failed Get: %+v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test(Item{Key: "Add_1", Value: []byte(`{"add": 12}`), Exp: 1}, false, [][]byte{[]byte(`{"add": 1}`)})
	test(Item{Key: "Add_2", Value: []byte(`{"add": 2}`), Exp: 1}, true, [][]byte{[]byte(`{"add": 2}`)})
}

func TestReplace(t *testing.T) {
	if _, err := cp.Set(Item{Key: "Replace_1", Value: []byte(`{"replace": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	test := func(item Item, eBool bool, evs [][]byte) {
		b, err := cp.Replace(item)
		if err != nil {
			t.Fatalf("Failed Replace: %+v", err)
		}
		assert.Equal(t, eBool, b)
		is, err := cp.Get(item.Key)
		if err != nil {
			t.Fatalf("Failed Get: %+v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test(Item{Key: "Replace_1", Value: []byte(`{"replace": 12}`), Exp: 1}, true, [][]byte{[]byte(`{"replace": 12}`)})
	test(Item{Key: "Replace_2", Value: []byte(`{"replace": 2}`), Exp: 1}, false, [][]byte{})
}

func TestAppend(t *testing.T) {
	if _, err := cp.Set(Item{Key: "Append_1", Value: []byte(`{"append": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	test := func(item Item, eBool bool, evs [][]byte) {
		b, err := cp.Append(item)
		if err != nil {
			t.Fatalf("Failed Append: %+v", err)
		}
		assert.Equal(t, eBool, b)
		is, err := cp.Get(item.Key)
		if err != nil {
			t.Fatalf("Failed Get: %+v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test(Item{Key: "Append_1", Value: []byte(`,{"append": 12}`), Exp: 1}, true, [][]byte{[]byte(`{"append": 1},{"append": 12}`)})
	test(Item{Key: "Append_2", Value: []byte(`{"append": 2}`), Exp: 1}, false, [][]byte{})
}

func TestPrepend(t *testing.T) {
	if _, err := cp.Set(Item{Key: "Prepend_1", Value: []byte(`{"prepend": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	test := func(item Item, eBool bool, evs [][]byte) {
		b, err := cp.Prepend(item)
		if err != nil {
			t.Fatalf("Failed Prepend: %+v", err)
		}
		assert.Equal(t, eBool, b)
		is, err := cp.Get(item.Key)
		if err != nil {
			t.Fatalf("Failed Get: %+v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test(Item{Key: "Prepend_1", Value: []byte(`{"prepend": 12}`), Exp: 1}, true, [][]byte{[]byte(`{"prepend": 12}{"prepend": 1}`)})
	test(Item{Key: "Prepend_2", Value: []byte(`{"prepend": 2}`), Exp: 1}, false, [][]byte{})
}

func TestCas(t *testing.T) {
	if _, err := cp.Set(Item{Key: "Cas_1", Value: []byte(`{"cas": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	if _, err := cp.Set(Item{Key: "Cas_2", Value: []byte(`{"cas": 2}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	test := func(pattern string, item Item, eBool bool, evs [][]byte) {
		is, err := cp.Gets(item.Key)
		if err != nil {
			t.Fatalf("Failed Gets: Key: %s, err: %+v", item.Key, err)
		}
		item.Cas = is[0].Cas
		if pattern == "before" {
			if _, err = cp.Append(Item{Key: item.Key, Value: []byte("update"), Exp: 1}); err != nil {
				t.Fatalf("Failed Append: %+v", err)
			}
		}
		b, err := cp.Cas(item)
		if err != nil {
			t.Fatalf("Failed Cas: %+v", err)
		}
		assert.Equal(t, eBool, b)
		is, err = cp.Gets(item.Key)
		if err != nil {
			t.Fatalf("Failed Gets: Key: %s, err: %+v", item.Key, err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test("after", Item{Key: "Cas_1", Value: []byte(`{"cas": 12}`), Exp: 1}, true, [][]byte{[]byte(`{"cas": 12}`)})
	test("before", Item{Key: "Cas_2", Value: []byte(`{"cas": 22}`), Exp: 1}, false, [][]byte{[]byte(`{"cas": 2}update`)})
}

func TestDelete(t *testing.T) {
	if _, err := cp.Set(Item{Key: "Delete_1", Value: []byte(`{"delete": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %+v", err)
	}
	test := func(key string, eBool bool, evs [][]byte) {
		deleted, err := cp.Delete(key)
		if err != nil {
			t.Fatalf("Failed Delete: %+v", err)
		}
		assert.Equal(t, eBool, deleted)
		is, err := cp.Get(key)
		if err != nil {
			t.Fatalf("Failed Get: %+v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test("Delete_1", true, [][]byte{})
	test("Delete_2", false, [][]byte{})
}
