package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteSortedMap extends ReadSortedMap {
    protected WriteSortedMap() {
    }

    public WriteSortedMap(WriteCursor cursor) throws Exception {
        super(cursor.writePath(new Database.PathPart[]{
            new Database.SortedMapInit(false)
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

    // methods that take a string key (encoded as UTF-8 bytes)

    public void put(String key, Database.WriteableData data) throws Exception {
        put(key.getBytes("UTF-8"), data);
    }

    public WriteCursor putCursor(String key) throws Exception {
        return putCursor(key.getBytes("UTF-8"));
    }

    public boolean remove(String key) throws Exception {
        return remove(key.getBytes("UTF-8"));
    }

    // methods that take a byte-array key directly

    public void put(byte[] key, Database.WriteableData data) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.SortedMapGet(new Database.SortedMapGetValue(key)),
            new Database.WriteData(data)
        });
    }

    public WriteCursor putCursor(byte[] key) throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.SortedMapGet(new Database.SortedMapGetValue(key))
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
