# Changelog

## Unreleased

### Fixed

- DAG state writes now publish persistent roots, so a previously captured `ReadState` no longer observes outputs added by later steps. Call `AsRead` again to read the current state. Snapshot values are shallow-shared; mutable values stored in state are not deep-copied.
