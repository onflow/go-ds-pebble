package pebbleds

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/ipfs/go-datastore"
	dstest "github.com/ipfs/go-datastore/test"
)

func TestPebbleDatastore(t *testing.T) {
	ds, cleanup := newDatastore(t)
	defer cleanup()

	dstest.SubtestAll(t, ds)
}

func newDatastore(t *testing.T, options ...Option) (*Datastore, func()) {
	t.Helper()

	path, err := os.MkdirTemp(os.TempDir(), "testing_pebble_")
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, options...)
	if err != nil {
		t.Fatal(err)
	}

	return d, func() {
		_ = d.Close()
		_ = os.RemoveAll(path)
	}
}

func newDatastoreWithPebbleDB(t *testing.T) (*Datastore, func()) {
	t.Helper()

	path, err := os.MkdirTemp(os.TempDir(), "testing_pebble_with_db")
	if err != nil {
		t.Fatal(err)
	}

	db, err := pebble.Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, WithPebbleDB(db))
	if err != nil {
		t.Fatal(err)
	}

	return d, func() {
		_ = d.Close()
		_ = os.RemoveAll(path)
	}
}

func TestGet(t *testing.T) {
	ds, cleanup := newDatastore(t)
	defer cleanup()

	testDatastore(t, ds)
}

func TestGetWithPebbleDB(t *testing.T) {
	ds, cleanup := newDatastoreWithPebbleDB(t)
	defer cleanup()

	testDatastore(t, ds)
}

func testDatastore(t *testing.T, ds *Datastore) {
	ctx := context.Background()
	k := datastore.NewKey("a")
	v := []byte("val")
	err := ds.Put(ctx, k, v)
	if err != nil {
		t.Fatal(err)
	}

	err = ds.Put(ctx, datastore.NewKey("aa"), v)
	if err != nil {
		t.Fatal(err)
	}

	err = ds.Put(ctx, datastore.NewKey("ac"), v)
	if err != nil {
		t.Fatal(err)
	}

	has, err := ds.Has(ctx, datastore.NewKey("ab"))
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("should not have key")
	}

	val, err := ds.Get(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, val) {
		t.Error("not equal", string(val))
	}
}

func TestPebbleWriteOptions(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		dstore, cleanup := newDatastore(t)
		defer cleanup()

		if dstore.writeOptions != pebble.NoSync {
			t.Fatalf("incorrect write options: expected %v, got %v", pebble.NoSync, dstore.writeOptions)
		}

		batch, err := dstore.Batch(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if batch.(*Batch).writeOptions != pebble.NoSync {
			t.Fatalf("incorrect batch write options: expected %v, got %v", pebble.NoSync, batch.(*Batch).writeOptions)
		}
	})

	t.Run("pebble.Sync", func(t *testing.T) {
		dstore, cleanup := newDatastore(t, WithPebbleWriteOptions(pebble.Sync))
		defer cleanup()

		if dstore.writeOptions != pebble.Sync {
			t.Fatalf("incorrect write options: expected %v, got %v", pebble.Sync, dstore.writeOptions)
		}

		batch, err := dstore.Batch(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		if batch.(*Batch).writeOptions != pebble.Sync {
			t.Fatalf("incorrect batch write options: expected %v, got %v", pebble.Sync, batch.(*Batch).writeOptions)
		}
	})
}
