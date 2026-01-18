package standalonestorage

import (
	"testing"

	"github.com/zbchi/mizu/kv/config"
	"github.com/zbchi/mizu/kv/storage"
)

func TestStandaloneStorageIsolatesUserDataByRegion(t *testing.T) {
	store := NewStandaloneStorage(&config.Config{DBPath: t.TempDir()})
	defer func() {
		if err := store.Stop(); err != nil {
			t.Fatalf("stop storage: %v", err)
		}
	}()

	key := []byte("shared-key")
	region1 := store.RegionStorage(1)
	region2 := store.RegionStorage(2)

	if err := region1.Write([]storage.Modify{
		{Data: storage.Put{Cf: "default", Key: key, Value: []byte("region-1")}},
	}); err != nil {
		t.Fatalf("write region 1: %v", err)
	}
	if err := region2.Write([]storage.Modify{
		{Data: storage.Put{Cf: "default", Key: key, Value: []byte("region-2")}},
	}); err != nil {
		t.Fatalf("write region 2: %v", err)
	}

	reader1, err := region1.Reader()
	if err != nil {
		t.Fatalf("reader region 1: %v", err)
	}
	defer reader1.Close()

	reader2, err := region2.Reader()
	if err != nil {
		t.Fatalf("reader region 2: %v", err)
	}
	defer reader2.Close()

	got1, err := reader1.GetCF("default", key)
	if err != nil {
		t.Fatalf("get region 1: %v", err)
	}
	got2, err := reader2.GetCF("default", key)
	if err != nil {
		t.Fatalf("get region 2: %v", err)
	}

	if string(got1) != "region-1" {
		t.Fatalf("region 1 value = %q, want %q", got1, "region-1")
	}
	if string(got2) != "region-2" {
		t.Fatalf("region 2 value = %q, want %q", got2, "region-2")
	}
}
