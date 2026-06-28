package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteSortedSet extends ReadSortedSet {
    protected WriteSortedSet() {
    }

    public WriteSortedSet(WriteCursor cursor) throws Exception {
        super(cursor.writePath(new Database.PathPart[]{
            new Database.SortedMapInit(true)
        }));
    }

    @Override
    public WriteCursor.Iterator iterator() {
        return ((WriteCursor)this.cursor).iterator();
    }

    @Override
    public WriteCursor.Iterator iteratorFrom(String startKey) throws Exception {
        return iteratorFrom(startKey.getBytes("UTF-8"));
    }

    @Override
    public WriteCursor.Iterator iteratorFrom(byte[] startKey) throws IOException {
        return WriteCursor.Iterator.from(ReadCursor.Iterator.initSortedFromKey(this.cursor, startKey));
    }

    @Override
    public WriteCursor.Iterator iteratorFromIndex(long startIndex) throws IOException {
        return WriteCursor.Iterator.from(ReadCursor.Iterator.initSortedFromIndex(this.cursor, startIndex));
    }

    public void put(String key) throws Exception {
        put(key.getBytes("UTF-8"));
    }

    public boolean remove(String key) throws Exception {
        return remove(key.getBytes("UTF-8"));
    }

    public void put(byte[] key) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.SortedMapGet(new Database.SortedMapGetKey(key))
        });
    }

    public boolean remove(byte[] key) throws Exception {
        try {
            ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
                new Database.SortedMapRemove(key)
            });
        } catch (Database.KeyNotFoundException e) {
            return false;
        }
        return true;
    }
}
