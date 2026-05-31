package storage

import (
	"testing"

	"github.com/zbchi/mizu/proto/raftpb"
	"github.com/zbchi/mizu/raft"
)

func TestLSMStorageRegionAndRaftPersistence(t *testing.T) {
	path := t.TempDir()
	store := NewLSMStorage(path)

	region1 := store.RegionStorage(1)
	region2 := store.RegionStorage(2)
	if err := region1.Write([]Modify{
		{Data: Put{Cf: "default", Key: []byte("a"), Value: []byte("1")}},
		{Data: Put{Cf: "default", Key: []byte("b"), Value: []byte("2")}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := region2.Write([]Modify{{Data: Put{Cf: "default", Key: []byte("a"), Value: []byte("other")}}}); err != nil {
		t.Fatal(err)
	}

	reader, err := region1.Reader()
	if err != nil {
		t.Fatal(err)
	}
	value, err := reader.GetCF("default", []byte("a"))
	if err != nil || string(value) != "1" {
		t.Fatalf("get region 1: value=%q err=%v", value, err)
	}
	if err := region1.Write([]Modify{{Data: Put{Cf: "default", Key: []byte("a"), Value: []byte("new")}}}); err != nil {
		t.Fatal(err)
	}
	value, err = reader.GetCF("default", []byte("a"))
	if err != nil || string(value) != "1" {
		t.Fatalf("reader did not preserve its snapshot: value=%q err=%v", value, err)
	}
	otherReader, err := region2.Reader()
	if err != nil {
		t.Fatal(err)
	}
	otherValue, err := otherReader.GetCF("default", []byte("a"))
	otherReader.Close()
	if err != nil || string(otherValue) != "other" {
		t.Fatalf("get region 2: value=%q err=%v", otherValue, err)
	}

	iterator := reader.IterCF("default")
	iterator.Seek(nil)
	var keys []string
	for iterator.Valid() {
		item, itemErr := iterator.Item()
		if itemErr != nil {
			t.Fatal(itemErr)
		}
		userKey, ok := DecodeUserKey(1, "default", item.Key)
		if !ok {
			t.Fatalf("unexpected encoded key %q", item.Key)
		}
		keys = append(keys, string(userKey))
		iterator.Next()
	}
	iterator.Close()
	reader.Close()
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("iterated keys: %v", keys)
	}

	snapshot, err := region1.CreateSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := region1.Write([]Modify{{Data: Delete{Cf: "default", Key: []byte("a")}}}); err != nil {
		t.Fatal(err)
	}
	if err := region1.ApplySnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	reader, err = region1.Reader()
	if err != nil {
		t.Fatal(err)
	}
	value, err = reader.GetCF("default", []byte("a"))
	reader.Close()
	if err != nil || string(value) != "new" {
		t.Fatalf("snapshot restore: value=%q err=%v", value, err)
	}
	otherReader, err = region2.Reader()
	if err != nil {
		t.Fatal(err)
	}
	otherValue, err = otherReader.GetCF("default", []byte("a"))
	otherReader.Close()
	if err != nil || string(otherValue) != "other" {
		t.Fatalf("region 2 changed while restoring region 1: value=%q err=%v", otherValue, err)
	}

	raftStore := store.RaftStorage(1)
	raftStore2 := store.RaftStorage(2)
	wantState := raft.HardState{Term: 3, Vote: 7, CommitIndex: 9}
	if err := raftStore.SaveHardState(wantState); err != nil {
		t.Fatal(err)
	}
	wantState2 := raft.HardState{Term: 4, Vote: 8, CommitIndex: 2}
	if err := raftStore2.SaveHardState(wantState2); err != nil {
		t.Fatal(err)
	}
	entries := []*raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("one")},
		{Index: 2, Term: 2, Data: []byte("two")},
		{Index: 3, Term: 3, Data: []byte("three")},
	}
	if err := raftStore.SaveEntries(entries); err != nil {
		t.Fatal(err)
	}
	entries2 := []*raftpb.Entry{{Index: 1, Term: 4, Data: []byte("other-one")}, {Index: 2, Term: 4, Data: []byte("other-two")}}
	if err := raftStore2.SaveEntries(entries2); err != nil {
		t.Fatal(err)
	}
	gotState, err := raftStore.LoadHardState()
	if err != nil || gotState != wantState {
		t.Fatalf("hard state: got=%+v err=%v", gotState, err)
	}
	gotEntries, err := raftStore.LoadEntries(1, 4)
	if err != nil || len(gotEntries) != 3 || string(gotEntries[2].Data) != "three" {
		t.Fatalf("entries: got=%v err=%v", gotEntries, err)
	}
	if err := raftStore.SaveSnapshot(&raftpb.Snapshot{Index: 2, Term: 2, Data: []byte("snapshot")}); err != nil {
		t.Fatal(err)
	}
	gotSnapshot, err := raftStore.LoadSnapshot()
	if err != nil || gotSnapshot == nil || string(gotSnapshot.Data) != "snapshot" {
		t.Fatalf("snapshot: got=%v err=%v", gotSnapshot, err)
	}
	if err := raftStore.Compact(3); err != nil {
		t.Fatal(err)
	}
	gotEntries, err = raftStore.LoadEntries(1, 4)
	if err != nil || len(gotEntries) != 1 || gotEntries[0].Index != 3 {
		t.Fatalf("compacted entries: got=%v err=%v", gotEntries, err)
	}
	gotEntries2, err := raftStore2.LoadEntries(1, 3)
	if err != nil || len(gotEntries2) != 2 {
		t.Fatalf("region 2 entries changed after compact: got=%v err=%v", gotEntries2, err)
	}
	if err := raftStore2.TruncateFrom(2); err != nil {
		t.Fatal(err)
	}
	gotEntries2, err = raftStore2.LoadEntries(1, 3)
	if err != nil || len(gotEntries2) != 1 || gotEntries2[0].Index != 1 {
		t.Fatalf("truncated entries: got=%v err=%v", gotEntries2, err)
	}

	if err := store.Stop(); err != nil {
		t.Fatal(err)
	}
	store = NewLSMStorage(path)
	defer store.Stop()
	reader, err = store.RegionStorage(1).Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	value, err = reader.GetCF("default", []byte("a"))
	if err != nil || string(value) != "new" {
		t.Fatalf("reopen: value=%q err=%v", value, err)
	}
	gotState, err = store.RaftStorage(1).LoadHardState()
	if err != nil || gotState != wantState {
		t.Fatalf("reopen hard state: got=%+v err=%v", gotState, err)
	}
	gotSnapshot, err = store.RaftStorage(1).LoadSnapshot()
	if err != nil || gotSnapshot == nil || string(gotSnapshot.Data) != "snapshot" {
		t.Fatalf("reopen snapshot: got=%v err=%v", gotSnapshot, err)
	}
}
