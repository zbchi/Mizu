package raftstorage

import (
	"testing"

	"github.com/dgraph-io/badger/v3"
	kvstorage "github.com/zbchi/mizu/kv/storage"
	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
	"google.golang.org/protobuf/proto"
)

func openTestDB(t *testing.T) *badger.DB {
	t.Helper()

	dir := t.TempDir()
	opts := badger.DefaultOptions(dir).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("close badger: %v", err)
		}
	})
	return db
}

func TestStorageIsolatesHardStateAndSnapshotByRegion(t *testing.T) {
	db := openTestDB(t)

	s1 := NewStorage(db, 1)
	s2 := NewStorage(db, 2)

	wantState1 := raft.HardState{Term: 1, Vote: 11, CommitIndex: 3}
	wantState2 := raft.HardState{Term: 2, Vote: 22, CommitIndex: 6}

	if err := s1.SaveHardState(wantState1); err != nil {
		t.Fatalf("save state 1: %v", err)
	}
	if err := s2.SaveHardState(wantState2); err != nil {
		t.Fatalf("save state 2: %v", err)
	}

	gotState1, err := s1.LoadHardState()
	if err != nil {
		t.Fatalf("load state 1: %v", err)
	}
	gotState2, err := s2.LoadHardState()
	if err != nil {
		t.Fatalf("load state 2: %v", err)
	}

	if gotState1 != wantState1 {
		t.Fatalf("state 1 mismatch: got %+v want %+v", gotState1, wantState1)
	}
	if gotState2 != wantState2 {
		t.Fatalf("state 2 mismatch: got %+v want %+v", gotState2, wantState2)
	}

	snap1 := &raftpb.Snapshot{Index: 5, Term: 1, Data: []byte("r1")}
	snap2 := &raftpb.Snapshot{Index: 8, Term: 2, Data: []byte("r2")}
	if err := s1.SaveSnapshot(snap1); err != nil {
		t.Fatalf("save snapshot 1: %v", err)
	}
	if err := s2.SaveSnapshot(snap2); err != nil {
		t.Fatalf("save snapshot 2: %v", err)
	}

	gotSnap1, err := s1.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot 1: %v", err)
	}
	gotSnap2, err := s2.LoadSnapshot()
	if err != nil {
		t.Fatalf("load snapshot 2: %v", err)
	}

	if gotSnap1.Index != snap1.Index || gotSnap1.Term != snap1.Term || string(gotSnap1.Data) != string(snap1.Data) {
		t.Fatalf("snapshot 1 mismatch: got %+v want %+v", gotSnap1, snap1)
	}
	if gotSnap2.Index != snap2.Index || gotSnap2.Term != snap2.Term || string(gotSnap2.Data) != string(snap2.Data) {
		t.Fatalf("snapshot 2 mismatch: got %+v want %+v", gotSnap2, snap2)
	}
}

func TestStorageIsolatesEntriesByRegion(t *testing.T) {
	db := openTestDB(t)

	s1 := NewStorage(db, 1)
	s2 := NewStorage(db, 2)

	entries1 := []*raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("r1-1")},
		{Index: 2, Term: 1, Data: []byte("r1-2")},
		{Index: 3, Term: 2, Data: []byte("r1-3")},
	}
	entries2 := []*raftpb.Entry{
		{Index: 1, Term: 3, Data: []byte("r2-1")},
		{Index: 2, Term: 3, Data: []byte("r2-2")},
	}

	if err := s1.SaveEntries(entries1); err != nil {
		t.Fatalf("save entries 1: %v", err)
	}
	if err := s2.SaveEntries(entries2); err != nil {
		t.Fatalf("save entries 2: %v", err)
	}

	got1, err := s1.LoadEntries(1, 4)
	if err != nil {
		t.Fatalf("load entries 1: %v", err)
	}
	got2, err := s2.LoadEntries(1, 3)
	if err != nil {
		t.Fatalf("load entries 2: %v", err)
	}

	if len(got1) != len(entries1) {
		t.Fatalf("entries 1 length mismatch: got %d want %d", len(got1), len(entries1))
	}
	if len(got2) != len(entries2) {
		t.Fatalf("entries 2 length mismatch: got %d want %d", len(got2), len(entries2))
	}

	for i := range entries1 {
		if got1[i].Index != entries1[i].Index || got1[i].Term != entries1[i].Term || string(got1[i].Data) != string(entries1[i].Data) {
			t.Fatalf("entry 1[%d] mismatch: got %+v want %+v", i, got1[i], entries1[i])
		}
	}
	for i := range entries2 {
		if got2[i].Index != entries2[i].Index || got2[i].Term != entries2[i].Term || string(got2[i].Data) != string(entries2[i].Data) {
			t.Fatalf("entry 2[%d] mismatch: got %+v want %+v", i, got2[i], entries2[i])
		}
	}

	if err := s1.Compact(3); err != nil {
		t.Fatalf("compact region 1: %v", err)
	}

	got1, err = s1.LoadEntries(1, 4)
	if err != nil {
		t.Fatalf("reload entries 1 after compact: %v", err)
	}
	got2, err = s2.LoadEntries(1, 3)
	if err != nil {
		t.Fatalf("reload entries 2 after compact: %v", err)
	}

	if len(got1) != 1 || got1[0].Index != 3 {
		t.Fatalf("region 1 compact result mismatch: got %+v", got1)
	}
	if len(got2) != len(entries2) {
		t.Fatalf("region 2 changed after region 1 compact: got %d want %d", len(got2), len(entries2))
	}

	if err := s2.TruncateFrom(2); err != nil {
		t.Fatalf("truncate region 2: %v", err)
	}

	got2, err = s2.LoadEntries(1, 3)
	if err != nil {
		t.Fatalf("reload entries 2 after truncate: %v", err)
	}
	if len(got2) != 1 || got2[0].Index != 1 {
		t.Fatalf("region 2 truncate result mismatch: got %+v", got2)
	}

	got1, err = s1.LoadEntries(1, 4)
	if err != nil {
		t.Fatalf("reload entries 1 after region 2 truncate: %v", err)
	}
	if len(got1) != 1 || got1[0].Index != 3 {
		t.Fatalf("region 1 changed after region 2 truncate: got %+v", got1)
	}
}

func TestSnapshotDataIsolatedByRegion(t *testing.T) {
	db := openTestDB(t)

	s1 := NewStorage(db, 1)
	s2 := NewStorage(db, 2)

	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(kvstorage.EncodeKey(1, []byte("alpha"), "default"), []byte("r1-alpha")); err != nil {
			return err
		}
		if err := txn.Set(kvstorage.EncodeKey(1, []byte("beta"), "write"), []byte("r1-beta")); err != nil {
			return err
		}
		if err := txn.Set(kvstorage.EncodeKey(2, []byte("alpha"), "default"), []byte("r2-alpha")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("seed db: %v", err)
	}

	data := s1.MakeSnapshotData()
	var sn raftpb.SnapshotData
	if err := proto.Unmarshal(data, &sn); err != nil {
		t.Fatalf("unmarshal snapshot data: %v", err)
	}

	if len(sn.Kvs) != 2 {
		t.Fatalf("snapshot kv count = %d, want 2", len(sn.Kvs))
	}
	for _, kv := range sn.Kvs {
		if got := string(kv.Key); got[:len("data/1/")] != "data/1/" {
			t.Fatalf("snapshot contains non-region-1 key: %q", got)
		}
	}

	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(kvstorage.EncodeKey(1, []byte("alpha"), "default"), []byte("mutated")); err != nil {
			return err
		}
		if err := txn.Delete(kvstorage.EncodeKey(1, []byte("beta"), "write")); err != nil {
			return err
		}
		if err := txn.Set(kvstorage.EncodeKey(2, []byte("alpha"), "default"), []byte("r2-still")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("mutate db: %v", err)
	}

	if err := s1.ApplySnapshotData(data); err != nil {
		t.Fatalf("apply snapshot data: %v", err)
	}

	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(kvstorage.EncodeKey(1, []byte("alpha"), "default"))
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if string(val) != "r1-alpha" {
			t.Fatalf("region 1 alpha = %q, want %q", val, "r1-alpha")
		}

		item, err = txn.Get(kvstorage.EncodeKey(1, []byte("beta"), "write"))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if string(val) != "r1-beta" {
			t.Fatalf("region 1 beta = %q, want %q", val, "r1-beta")
		}

		item, err = txn.Get(kvstorage.EncodeKey(2, []byte("alpha"), "default"))
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if string(val) != "r2-still" {
			t.Fatalf("region 2 alpha = %q, want %q", val, "r2-still")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify db: %v", err)
	}

	_ = s2
}
