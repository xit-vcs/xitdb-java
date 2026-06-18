package io.github.radarroark.xitdb;

import java.io.IOException;

// a sorted set of byte-string keys (a SortedMap with no values).
public class ReadSortedSet implements Slotted, Iterable<ReadCursor> {
    public ReadCursor cursor;

    protected ReadSortedSet() {
    }

    public ReadSortedSet(ReadCursor cursor) {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, SORTED_MAP, SORTED_SET -> {
                this.cursor = cursor;
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    @Override
    public Slot slot() {
        return cursor.slot();
    }

    public long count() throws IOException {
        return this.cursor.count();
    }

    @Override
    public ReadCursor.Iterator iterator() {
        return this.cursor.iterator();
    }

    public ReadCursor.Iterator iteratorFrom(String startKey) throws Exception {
        return iteratorFrom(startKey.getBytes("UTF-8"));
    }

    public ReadCursor.Iterator iteratorFrom(byte[] startKey) throws IOException {
        return ReadCursor.Iterator.initSortedFromKey(this.cursor, startKey);
    }

    public ReadCursor.Iterator iteratorFromIndex(long startIndex) throws IOException {
        return ReadCursor.Iterator.initSortedFromIndex(this.cursor, startIndex);
    }

    // the key/value pair at the given rank (negative counts from the end)
    public ReadCursor.KeyValuePairCursor getIndexKeyValuePair(long index) throws Exception {
        var cursor = this.cursor.readPath(new Database.PathPart[]{
            new Database.SortedMapGetIndex(index)
        });
        if (cursor == null) {
            return null;
        } else {
            return cursor.readKeyValuePair();
        }
    }

    public boolean contains(String key) throws Exception {
        return contains(key.getBytes("UTF-8"));
    }

    public long rank(String key) throws Exception {
        return rank(key.getBytes("UTF-8"));
    }

    public boolean contains(byte[] key) throws Exception {
        return this.cursor.readPath(new Database.PathPart[]{
            new Database.SortedMapGet(new Database.SortedMapGetKey(key))
        }) != null;
    }

    // number of keys strictly less than key
    public long rank(byte[] key) throws IOException {
        if (this.cursor.slotPtr.slot().tag() == Tag.NONE) return 0;
        this.cursor.db.core.seek(this.cursor.slotPtr.slot().value());
        var reader = this.cursor.db.core.reader();
        var headerBytes = new byte[Database.BTreeHeader.length];
        reader.readFully(headerBytes);
        var header = Database.BTreeHeader.fromBytes(headerBytes);
        return this.cursor.db.sortedRank(header.rootPtr(), key);
    }
}
