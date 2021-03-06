package memcached

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestClient_Get(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "Get_1", Value: []byte(`{"get": 1}`), Exp: 10}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(keys []string, eis []*Item) {
		is, err := cl.Get(keys...)
		if err != nil {
			t.Fatalf("Failed Get: %v", err)
		}
		assert.Equal(t, eis, is)
	}
	test([]string{"Get_1"}, []*Item{{Key: "Get_1", Value: []byte(`{"get": 1}`)}})
	test([]string{"Get_2"}, []*Item{})
	test([]string{"Get_1", "Get_2"}, []*Item{{Key: "Get_1", Value: []byte(`{"get": 1}`)}})
}

func TestClient_GetOrSet(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "GetOrSet_1", Value: []byte(`{"get_or_set": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(key string, eitem *Item) {
		item, err := cl.GetOrSet(key, func(key string) (*Item, error) {
			return &Item{Key: key, Value: []byte(`{"get_or_set": 2}`)}, nil
		})
		if err != nil {
			t.Fatalf("Failed GetOrSet: %v", err)
		}
		assert.Equal(t, eitem, item)
	}
	test("GetOrSet_1", &Item{Key: "GetOrSet_1", Value: []byte(`{"get_or_set": 1}`)})
	test("GetOrSet_2", &Item{Key: "GetOrSet_2", Value: []byte(`{"get_or_set": 2}`)})
}

func TestClient_GetOrSetMulti(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "GetOrSetM_1", Value: []byte(`{"get_or_set_m": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(keys []string, cb func(keys []string) ([]*Item, error), eis []*Item) {
		items, err := cl.GetOrSetMulti(keys, cb)
		if err != nil {
			t.Fatalf("Failed GetOrSetMulti: %v", err)
		}
		assert.Equal(t, eis, items)
	}
	test(
		[]string{"GetOrSetM_1"},
		func(keys []string) ([]*Item, error) {
			return []*Item{}, nil
		},
		[]*Item{{Key: "GetOrSetM_1", Value: []byte(`{"get_or_set_m": 1}`)}},
	)
	test(
		[]string{"GetOrSetM_1", "GetOrSetM_2"},
		func(keys []string) ([]*Item, error) {
			return []*Item{{Key: "GetOrSetM_2", Value: []byte(`{"get_or_set_m": 2}`)}}, nil
		},
		[]*Item{{Key: "GetOrSetM_1", Value: []byte(`{"get_or_set_m": 1}`)}, {Key: "GetOrSetM_2", Value: []byte(`{"get_or_set_m": 2}`)}},
	)
	test(
		[]string{"GetOrSetM_1", "GetOrSetM_12", "GetOrSetM_13", "GetOrSetM_14"},
		func(keys []string) ([]*Item, error) {
			return []*Item{
				{Key: "GetOrSetM_12", Value: []byte(`{"get_or_set_m": 12}`)},
				{Key: "GetOrSetM_13", Value: []byte(`{"get_or_set_m": 13}`)},
				{Key: "GetOrSetM_14", Value: []byte(`{"get_or_set_m": 14}`)},
			}, nil
		},
		[]*Item{
			{Key: "GetOrSetM_1", Value: []byte(`{"get_or_set_m": 1}`)},
			{Key: "GetOrSetM_12", Value: []byte(`{"get_or_set_m": 12}`)},
			{Key: "GetOrSetM_13", Value: []byte(`{"get_or_set_m": 13}`)},
			{Key: "GetOrSetM_14", Value: []byte(`{"get_or_set_m": 14}`)},
		})
}

func TestClient_Gets(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "Gets_1", Value: []byte(`{"gets": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(key string, evs [][]byte) {
		is, err := cl.Gets(key)
		if err != nil {
			t.Fatalf("Failed Gets: %v", err)
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

func TestClient_Gat(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "Gat_1", Value: []byte(`{"gat": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(keys []string, exp int64, evs [][]byte) {
		is, err := cl.Gat(exp, keys...)
		if err != nil {
			fmt.Printf("Error type: %T\n", errors.Cause(err))
			t.Fatalf("Failed Gat: %v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test([]string{"Gat_1"}, 1, [][]byte{[]byte(`{"gat": 1}`)})
	test([]string{"Gat_2"}, 1, [][]byte{})
	keys := make([]string, 201)
	for i := 0; i < 201; i++ {
		keys[i] = fmt.Sprintf("Gat_%d", i)
	}
	test(keys, 2, [][]byte{[]byte(`{"gat": 1}`)})
}

func TestClient_GatOrSet(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "GatOrSet_1", Value: []byte(`{"gat_or_set": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(key string, eitem *Item) {
		item, err := cl.GatOrSet(key, 1, func(key string) (*Item, error) {
			return &Item{Key: key, Value: []byte(`{"gat_or_set": 2}`)}, nil
		})
		if err != nil {
			t.Fatalf("Failed GatOrSet: %v", err)
		}
		assert.Equal(t, eitem, item)
	}
	test("GatOrSet_1", &Item{Key: "GatOrSet_1", Value: []byte(`{"gat_or_set": 1}`)})
	test("GatOrSet_2", &Item{Key: "GatOrSet_2", Value: []byte(`{"gat_or_set": 2}`)})
}

func TestClient_GatOrSetMulti(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "GatOrSetM_1", Value: []byte(`{"gat_or_set_m": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(keys []string, cb func(keys []string) ([]*Item, error), eis []*Item) {
		items, err := cl.GatOrSetMulti(keys, 1, cb)
		if err != nil {
			t.Fatalf("Failed GatOrSetMulti: %v", err)
		}
		assert.Equal(t, eis, items)
	}
	test(
		[]string{"GatOrSetM_1"},
		func(keys []string) ([]*Item, error) {
			return []*Item{}, nil
		},
		[]*Item{{Key: "GatOrSetM_1", Value: []byte(`{"gat_or_set_m": 1}`)}},
	)
	test(
		[]string{"GatOrSetM_1", "GatOrSetM_2"},
		func(keys []string) ([]*Item, error) {
			return []*Item{{Key: "GatOrSetM_2", Value: []byte(`{"gat_or_set_m": 2}`)}}, nil
		},
		[]*Item{{Key: "GatOrSetM_1", Value: []byte(`{"gat_or_set_m": 1}`)}, {Key: "GatOrSetM_2", Value: []byte(`{"gat_or_set_m": 2}`)}},
	)
	test(
		[]string{"GatOrSetM_1", "GatOrSetM_12", "GatOrSetM_13", "GatOrSetM_14"},
		func(keys []string) ([]*Item, error) {
			return []*Item{
				{Key: "GatOrSetM_12", Value: []byte(`{"gat_or_set_m": 12}`)},
				{Key: "GatOrSetM_13", Value: []byte(`{"gat_or_set_m": 13}`)},
				{Key: "GatOrSetM_14", Value: []byte(`{"gat_or_set_m": 14}`)},
			}, nil
		},
		[]*Item{
			{Key: "GatOrSetM_1", Value: []byte(`{"gat_or_set_m": 1}`)},
			{Key: "GatOrSetM_12", Value: []byte(`{"gat_or_set_m": 12}`)},
			{Key: "GatOrSetM_13", Value: []byte(`{"gat_or_set_m": 13}`)},
			{Key: "GatOrSetM_14", Value: []byte(`{"gat_or_set_m": 14}`)},
		})
}

func TestClient_Gats(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "Gats_1", Value: []byte(`{"gats": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(key string, exp int64, evs [][]byte) {
		is, err := cl.Gats(exp, key)
		if err != nil {
			t.Fatalf("Failed Gats: %v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test("Gats_1", 1, [][]byte{[]byte(`{"gats": 1}`)})
	test("Gats_2", 1, [][]byte{})
}

func TestClient_Set(t *testing.T) {
	test := func(items []*Item, noreply bool) {
		failedKeys, err := cl.Set(noreply, items...)
		if err != nil {
			t.Fatalf("Failed Set: %v", err)
		}
		assert.Len(t, failedKeys, 0)
	}
	test([]*Item{
		{Key: "set_1", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1},
	}, false)
	test([]*Item{
		{Key: "set_2", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1},
		{Key: "set_3", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1},
	}, false)
	test([]*Item{
		{Key: "set_4", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1},
		{Key: "set_5", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1},
	}, true)
}

func TestClient_Add(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "Add_1", Value: []byte(`{"add": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(item *Item, eBool bool, evs [][]byte) {
		failedKeys, err := cl.Add(false, item)
		if err != nil {
			t.Fatalf("Failed Add: %v", err)
		}
		assert.Equal(t, eBool, len(failedKeys) < 1)
		is, err := cl.Get(item.Key)
		if err != nil {
			t.Fatalf("Failed Get: %v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test(&Item{Key: "Add_1", Value: []byte(`{"add": 12}`), Exp: 1}, false, [][]byte{[]byte(`{"add": 1}`)})
	test(&Item{Key: "Add_2", Value: []byte(`{"add": 2}`), Exp: 1}, true, [][]byte{[]byte(`{"add": 2}`)})
}

func TestClient_Replace(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "Replace_1", Value: []byte(`{"replace": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(item *Item, eBool bool, evs [][]byte) {
		failedKeys, err := cl.Replace(false, item)
		if err != nil {
			t.Fatalf("Failed Replace: %v", err)
		}
		assert.Equal(t, eBool, len(failedKeys) < 1)
		is, err := cl.Get(item.Key)
		if err != nil {
			t.Fatalf("Failed Get: %v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test(&Item{Key: "Replace_1", Value: []byte(`{"replace": 12}`), Exp: 1}, true, [][]byte{[]byte(`{"replace": 12}`)})
	test(&Item{Key: "Replace_2", Value: []byte(`{"replace": 2}`), Exp: 1}, false, [][]byte{})
}

func TestClient_Append(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "Append_1", Value: []byte(`{"append": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(item *Item, eBool bool, evs [][]byte) {
		failedKeys, err := cl.Append(false, item)
		if err != nil {
			t.Fatalf("Failed Append: %v", err)
		}
		assert.Equal(t, eBool, len(failedKeys) < 1)
		is, err := cl.Get(item.Key)
		if err != nil {
			t.Fatalf("Failed Get: %v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test(&Item{Key: "Append_1", Value: []byte(`,{"append": 12}`), Exp: 1}, true, [][]byte{[]byte(`{"append": 1},{"append": 12}`)})
	test(&Item{Key: "Append_2", Value: []byte(`{"append": 2}`), Exp: 1}, false, [][]byte{})
}

func TestClient_Prepend(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "Prepend_1", Value: []byte(`{"prepend": 1}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(item *Item, eBool bool, evs [][]byte) {
		failedKeys, err := cl.Prepend(false, item)
		if err != nil {
			t.Fatalf("Failed Prepend: %v", err)
		}
		assert.Equal(t, eBool, len(failedKeys) < 1)
		is, err := cl.Get(item.Key)
		if err != nil {
			t.Fatalf("Failed Get: %v", err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test(&Item{Key: "Prepend_1", Value: []byte(`{"prepend": 12}`), Exp: 1}, true, [][]byte{[]byte(`{"prepend": 12}{"prepend": 1}`)})
	test(&Item{Key: "Prepend_2", Value: []byte(`{"prepend": 2}`), Exp: 1}, false, [][]byte{})
}

func TestClient_Cas(t *testing.T) {
	items := []*Item{
		{Key: "Cas_1", Value: []byte(`{"cas": 1}`), Exp: 10},
		{Key: "Cas_2", Value: []byte(`{"cas": 2}`), Exp: 10},
	}
	if _, err := cl.Set(true, items...); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	test := func(pattern string, item *Item, eBool bool, evs [][]byte) {
		is, err := cl.Gets(item.Key)
		if err != nil {
			t.Fatalf("Failed Gets: Key: %s, err: %v", item.Key, err)
		}
		if len(is) == 0 {
			t.Fatalf("cache miss")
		}
		item.Cas = is[0].Cas
		if pattern == "before" {
			if _, err = cl.Append(false, &Item{Key: item.Key, Value: []byte("update"), Exp: 1}); err != nil {
				t.Fatalf("Failed Append: %v", err)
			}
		}
		failedKeys, err := cl.Cas(false, item)
		if err != nil {
			t.Fatalf("Failed Cas: %v", err)
		}
		assert.Equal(t, eBool, len(failedKeys) < 1)
		is, err = cl.Gets(item.Key)
		if err != nil {
			t.Fatalf("Failed Gets: Key: %s, err: %v", item.Key, err)
		}
		vs := make([][]byte, len(is))
		for i, item := range is {
			vs[i] = item.Value
		}
		assert.Equal(t, evs, vs)
	}
	test("after", &Item{Key: "Cas_1", Value: []byte(`{"cas": 12}`), Exp: 1}, true, [][]byte{[]byte(`{"cas": 12}`)})
	test("before", &Item{Key: "Cas_2", Value: []byte(`{"cas": 22}`), Exp: 1}, false, [][]byte{[]byte(`{"cas": 2}update`)})
}

func TestClient_Touch(t *testing.T) {
	if _, err := cl.Set(false, &Item{Key: "touch_1", Value: []byte(`{"id": 1, "test": "ok"}`), Exp: 1}); err != nil {
		t.Fatalf("Failed Set: %v", err)
	}
	if err := cl.Touch("touch_1", 1, false); err != nil {
		t.Fatalf("Failed Touch: %v", err)
	}
	// noreply
	if err := cl.Touch("touch_1", 1, true); err != nil {
		t.Fatalf("Failed Touch: %v", err)
	}
	// not found
	err := cl.Touch("touch_not_found", 1, false)
	assert.Equal(t, ErrNotFound, err)
	// not found (noreply)
	err = cl.Touch("touch_not_found_no_reply", 1, true)
	assert.Nil(t, err)
}

func TestClient_Delete(t *testing.T) {
	test := func(noreply bool, keys []string, eBool bool) {
		failedKeys, err := cl.Delete(noreply, keys...)
		if err != nil {
			t.Fatalf("Failed Delete: %v", err)
		}
		assert.Equal(t, eBool, len(failedKeys) < 1)
		is, err := cl.Get(keys...)
		if err != nil {
			t.Fatalf("Failed Get: %v", err)
		}
		assert.Len(t, is, 0)
	}
	items := []*Item{
		{Key: "Delete_1", Value: []byte(`{"delete": 1}`), Exp: 1},
		{Key: "Delete_2", Value: []byte(`{"delete": 2}`), Exp: 1},
	}
	_, _ = cl.Set(false, items...)
	test(false, []string{"Delete_1", "Delete_2", "Delete_3"}, false)
	_, _ = cl.Set(false, items...)
	test(true, []string{"Delete_1", "Delete_2", "Delete_3"}, true)
}

func TestClient_Stats(t *testing.T) {
	test := func(argument string) {
		resultMap, err := cl.Stats(argument)
		if err != nil {
			t.Fatalf("Failed Stats: %v", err)
		}
		assert.NotEmpty(t, resultMap)
	}
	test("")
	test("items")
}
