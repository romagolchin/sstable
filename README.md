![example workflow](https://github.com/romagolchin/sstable/actions/workflows/maven.yml/badge.svg)

A prototype implementation of a key-value store as described in "Designing Data Intensive Applications", Martin
Kleppmann.

Basic storage unit is an append-only log file.

There are two implementation variants:

1. Assume that all keys fit in memory. Store values in log files, store mapping of keys to offsets (index) in memory (
   HashIndexLogFile).
2. Store key-value pairs in files and store sparse index in memory (SSTableLogFile).

In both variants a background job compacts the files, i.e. merges them and removes duplicated keys.

When the store is closed, it dumps its indices to disk to restore them on next use.