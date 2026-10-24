package io.github.radarroark.xitdb;

import java.nio.ByteBuffer;

/**
 * stores compaction offsets in a xitdb hash map instead of retaining them in
 * memory. the map should be in a separate scratch database, and the compaction
 * should run inside one transaction on it (see Database.runContext), so each
 * put isn't committed separately. reset clears the map.
 */
public final class FileOffsetMap implements OffsetMap {
    private WriteHashMap map;

    public FileOffsetMap(WriteHashMap map) {
        this.map = map;
    }

    @Override
    public void reset() throws Exception {
        var cursor = (WriteCursor) this.map.cursor;
        cursor.write(null);
        this.map = new WriteHashMap(cursor);
    }

    @Override
    public Long get(long sourceOffset) throws Exception {
        var cursor = this.map.getCursor(key(sourceOffset));
        return cursor == null ? null : cursor.readUint();
    }

    @Override
    public void put(long sourceOffset, long targetOffset) throws Exception {
        this.map.put(key(sourceOffset), new Database.Uint(targetOffset));
    }

    private byte[] key(long offset) {
        // encode the offset directly rather than hashing it, preserving exact keys
        var hashSize = this.map.cursor.db.header.hashSize();
        return ByteBuffer.allocate(hashSize).putLong(hashSize - Long.BYTES, offset).array();
    }
}
