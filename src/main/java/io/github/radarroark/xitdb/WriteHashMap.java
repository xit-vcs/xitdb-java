package io.github.radarroark.xitdb;

public class WriteHashMap extends ReadHashMap {
    protected WriteHashMap() {
    }

    public WriteHashMap(WriteCursor cursor) throws Exception {
        super(cursor.writePath(new Database.PathPart[]{
            new Database.HashMapInit(false, false)
        }));
    }

    @Override
    public WriteCursor.Iterator iterator() {
        return ((WriteCursor)this.cursor).iterator();
    }

    // methods that take a string key and hash it for you

    public void put(String key, Database.WriteableData data) throws Exception {
        var hash = this.cursor.db.hash(key.getBytes("UTF-8"));
        // this overload also stores the key
        putKey(hash, new Database.Bytes(key));
        put(hash, data);
    }

    public WriteCursor putCursor(String key) throws Exception {
        var hash = this.cursor.db.hash(key.getBytes("UTF-8"));
        // this overload also stores the key
        putKey(hash, new Database.Bytes(key));
        return putCursor(hash);
    }

    public void putKey(String key, Database.WriteableData data) throws Exception {
        putKey(this.cursor.db.hash(key.getBytes("UTF-8")), data);
    }

    public WriteCursor putKeyCursor(String key) throws Exception {
        return putKeyCursor(this.cursor.db.hash(key.getBytes("UTF-8")));
    }

    public boolean remove(String key) throws Exception {
        return remove(this.cursor.db.hash(key.getBytes("UTF-8")));
    }

    // methods that take a Database.Bytes key and hash it for you

    public void put(Database.Bytes key, Database.WriteableData data) throws Exception {
        var hash = this.cursor.db.hash(key.value());
        // this overload also stores the key
        putKey(hash, key);
        put(hash, data);
    }

    public WriteCursor putCursor(Database.Bytes key) throws Exception {
        var hash = this.cursor.db.hash(key.value());
        // this overload also stores the key
        putKey(hash, key);
        return putCursor(hash);
    }

    public void putKey(Database.Bytes key, Database.WriteableData data) throws Exception {
        putKey(this.cursor.db.hash(key.value()), data);
    }

    public WriteCursor putKeyCursor(Database.Bytes key) throws Exception {
        return putKeyCursor(this.cursor.db.hash(key.value()));
    }

    public boolean remove(Database.Bytes key) throws Exception {
        return remove(this.cursor.db.hash(key.value()));
    }

    // methods that take a hash directly

    public void put(byte[] hash, Database.WriteableData data) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetValue(hash)),
            new Database.WriteData(data)
        });
    }

    public WriteCursor putCursor(byte[] hash) throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetValue(hash)),
        });
    }

    public void putKey(byte[] hash, Database.WriteableData data) throws Exception {
        var cursor = ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash)),
        });
        // keys are only written if empty, because their value should always
        // be the same at a given hash.
        cursor.writeIfEmpty(data);
    }

    public WriteCursor putKeyCursor(byte[] hash) throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash)),
        });
    }

    public boolean remove(byte[] hash) throws Exception {
        try {
            ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
                new Database.HashMapRemove(hash)
            });
        } catch (Database.KeyNotFoundException e) {
            return false;
        }
        return true;
    }
}
