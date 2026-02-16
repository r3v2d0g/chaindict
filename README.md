## `chaindict`

A chain of dictionaries.

This crate implements a storage format which allows to create chains of
dictionaries (called links), each extending the previous links in the chain by
adding new unique entries which are each mapped to a unique `u32`.

Links are stored with snapshot and/or delta files, where snapshots contain the
list of all entries which have been inserted into this link and the previous
ones, and deltas only contain the list of entries which have been inserted into
this link.

The goals are:
1. to maintain a dictionary of values, mapping those to `u32`s; and
2. to allow using a storage backend like S3 (i.e. where files cannot be modified)
   to maintain this dictionary; and
3. to still be able to extend the dictionary at any point with new entries; and
4. to efficiently get all of the entries from the storage backend; and
5. to efficiently get only the new entries from the storage backend.
