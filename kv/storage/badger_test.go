package storage

import (
	"bytes"
	"testing"

	"github.com/zbchi/mizu/proto/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestBadgerStorageIsolatesUserDataByRegion(t *testing.T) {
	store := NewBadgerStorage(t.TempDir())
	defer func() {
		if err := store.Stop(); err != nil {
			t.Fatalf("stop storage: %v", err)
		}
	}()

	key := []byte("shared-key")
	region1 := store.RegionStorage(1)
	region2 := store.RegionStorage(2)

	if err := region1.Write([]Modify{{Data: Put{Cf: "default", Key: key, Value: []byte("region-1")}}}); err != nil {
		t.Fatalf("write region 1: %v", err)
	}
	if err := region2.Write([]Modify{{Data: Put{Cf: "default", Key: key, Value: []byte("region-2")}}}); err != nil {
		t.Fatalf("write region 2: %v", err)
	}

	assertRegionValue(t, region1, "default", key, "region-1")
	assertRegionValue(t, region2, "default", key, "region-2")
}

func TestBadgerRegionSnapshotIsIsolated(t *testing.T) {
	store := NewBadgerStorage(t.TempDir())
	defer func() { _ = store.Stop() }()

	region1 := store.RegionStorage(1)
	region2 := store.RegionStorage(2)
	requireWrite := func(region RegionStorage, mods ...Modify) {
		t.Helper()
		if err := region.Write(mods); err != nil {
			t.Fatalf("write region data: %v", err)
		}
	}

	requireWrite(region1,
		Modify{Data: Put{Cf: "default", Key: []byte("alpha"), Value: []byte("r1-alpha")}},
		Modify{Data: Put{Cf: "write", Key: []byte("beta"), Value: []byte("r1-beta")}},
	)
	requireWrite(region2,
		Modify{Data: Put{Cf: "default", Key: []byte("alpha"), Value: []byte("r2-alpha")}},
	)

	data, err := region1.CreateSnapshot()
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	var snapshot raftpb.SnapshotData
	if err := proto.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if len(snapshot.Kvs) != 2 {
		t.Fatalf("snapshot kv count = %d, want 2", len(snapshot.Kvs))
	}
	for _, kv := range snapshot.Kvs {
		if !bytes.HasPrefix(kv.Key, RegionDataPrefix(1)) {
			t.Fatalf("snapshot contains another region: %q", kv.Key)
		}
	}

	requireWrite(region1,
		Modify{Data: Put{Cf: "default", Key: []byte("alpha"), Value: []byte("mutated")}},
		Modify{Data: Delete{Cf: "write", Key: []byte("beta")}},
	)
	requireWrite(region2,
		Modify{Data: Put{Cf: "default", Key: []byte("alpha"), Value: []byte("r2-still")}},
	)

	if err := region1.ApplySnapshot(data); err != nil {
		t.Fatalf("apply snapshot: %v", err)
	}

	assertRegionValue(t, region1, "default", []byte("alpha"), "r1-alpha")
	assertRegionValue(t, region1, "write", []byte("beta"), "r1-beta")
	assertRegionValue(t, region2, "default", []byte("alpha"), "r2-still")
}

func assertRegionValue(t *testing.T, region RegionStorage, cf string, key []byte, want string) {
	t.Helper()
	reader, err := region.Reader()
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	defer reader.Close()

	got, err := reader.GetCF(cf, key)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("value for %q = %q, want %q", key, got, want)
	}
}
