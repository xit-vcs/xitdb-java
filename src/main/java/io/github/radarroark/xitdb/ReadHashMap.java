package io.github.radarroark.xitdb;

public class ReadHashMap implements Slotted, Iterable<ReadCursor> {
    public ReadCursor cursor;

    protected ReadHashMap() {
    }

    public ReadHashMap(ReadCursor cursor) {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, HASH_MAP, HASH_SET -> {
                this.cursor = cursor;
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    @Override
    public Slot slot() {
        return cursor.slot();
    }

    @Override
    public ReadCursor.Iterator iterator() {
        return this.cursor.iterator();
    }

    // methods that take a string key and hash it for you

    public ReadCursor getCursor(String key) throws Exception {
        return getCursor(this.cursor.db.hash(key.getBytes("UTF-8")));
    }

    public Slot getSlot(String key) throws Exception {
        return getSlot(this.cursor.db.hash(key.getBytes("UTF-8")));
    }

    public ReadCursor getKeyCursor(String key) throws Exception {
        return getKeyCursor(this.cursor.db.hash(key.getBytes("UTF-8")));
    }

    public Slot getKeySlot(String key) throws Exception {
        return getKeySlot(this.cursor.db.hash(key.getBytes("UTF-8")));
    }

    public ReadCursor.KeyValuePairCursor getKeyValuePair(String key) throws Exception {
        return getKeyValuePair(this.cursor.db.hash(key.getBytes("UTF-8")));
    }

    // methods that take a Database.Bytes key and hash it for you

    public ReadCursor getCursor(Database.Bytes key) throws Exception {
        return getCursor(this.cursor.db.hash(key.value()));
    }

    public Slot getSlot(Database.Bytes key) throws Exception {
        return getSlot(this.cursor.db.hash(key.value()));
    }

    public ReadCursor getKeyCursor(Database.Bytes key) throws Exception {
        return getKeyCursor(this.cursor.db.hash(key.value()));
    }

    public Slot getKeySlot(Database.Bytes key) throws Exception {
        return getKeySlot(this.cursor.db.hash(key.value()));
    }

    public ReadCursor.KeyValuePairCursor getKeyValuePair(Database.Bytes key) throws Exception {
        return getKeyValuePair(this.cursor.db.hash(key.value()));
    }

    // methods that take a hash directly

    public ReadCursor getCursor(byte[] hash) throws Exception {
        return this.cursor.readPath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetValue(hash))
        });
    }

    public Slot getSlot(byte[] hash) throws Exception {
        return this.cursor.readPathSlot(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetValue(hash))
        });
    }

    public ReadCursor getKeyCursor(byte[] hash) throws Exception {
        return this.cursor.readPath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash))
        });
    }

    public Slot getKeySlot(byte[] hash) throws Exception {
        return this.cursor.readPathSlot(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash))
        });
    }

    public ReadCursor.KeyValuePairCursor getKeyValuePair(byte[] hash) throws Exception {
        var cursor = this.cursor.readPath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKVPair(hash))
        });
        if (cursor == null) {
            return null;
        } else {
            return cursor.readKeyValuePair();
        }
    }
}
