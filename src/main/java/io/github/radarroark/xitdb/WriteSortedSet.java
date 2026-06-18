package io.github.radarroark.xitdb;

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
