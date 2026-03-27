# Database Directory

The database directory will use LevelDB-style file names:

```text
LOCK
CURRENT
MANIFEST-000001
000001.log
000002.sst
000003.tmp
```

`CURRENT` will be the entry point for recovery and name the active manifest.
Files are written to a numbered `.tmp` path, synced as required, and published
with an atomic rename. Day 1 creates only `LOCK`; WAL, MANIFEST, and SSTable
files are introduced in subsequent milestones.
