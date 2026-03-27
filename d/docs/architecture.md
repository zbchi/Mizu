# Architecture

`lsmtree` is a single-process ordered key-value store for Linux. Its public API
accepts arbitrary byte strings and returns `Status` for every recoverable error.

## Interface And Validation Rules

Validation belongs at trust boundaries, not at every layer of an internal call
chain:

- Public APIs validate invalid arguments that callers can normally provide and
  report recoverable failures with `Status`.
- Persistent WAL, SSTable, and manifest input is fully checked for invalid
  lengths, checksums, encodings, and sequence numbers.
- Required internal parameters use references, value types, and RAII instead of
  nullable raw pointers followed by repeated null checks.
- Internal programmer errors are expressed through types or assertions, not as
  recoverable runtime errors.
- Whether empty input is valid follows domain semantics: empty keys and values
  are valid, an empty WriteBatch is a no-op, and an empty WAL payload violates
  the on-disk format.

Each condition is checked once, by the layer closest to its trust boundary.

## Day 1

The serving state is an in-memory ordered map protected by a `std::shared_mutex`.
Reads use a shared lock and `WriteBatch` updates use an exclusive lock, making a
batch atomically visible. `put` and `erase` are convenience wrappers around
`write`, so all mutations have one future durability boundary.

Opening a database creates or validates its directory and holds an advisory
`LOCK` file for the lifetime of the `DB` handle. Day 1 intentionally has no WAL:
data is lost when the handle is destroyed.

## Planned Write Path

`WriteBatch -> sequence assignment -> WAL append -> optional fdatasync -> MemTable`

## Planned Read Path

`MemTable -> immutable MemTable -> L0 SSTables -> L1+ SSTables`
