package io.github.radarroark.xitdb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

/**
 * stores compaction offsets in a separate scratch database without retaining
 * the mappings in memory. the supplied file is truncated on construction and
 * reset, and is closed on close or failed construction. the caller deletes it.
 */
public final class FileOffsetMap implements OffsetMap, AutoCloseable {
    private static final int HASH_SIZE = 32;

    private final CoreFile core;
    private final Hasher hasher;
    private Database db;

    public FileOffsetMap(RandomAccessFile file) throws Exception {
        this.core = new CoreFile(file) {
            @Override
            public void sync() {
                // scratch mappings do not need crash durability
            }
        };
        try {
            this.hasher = new Hasher(MessageDigest.getInstance("SHA-256"));
            reset();
        } catch (Exception | Error e) {
            try {
                this.core.close();
            } catch (IOException closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    @Override
    public void reset() throws IOException {
        this.core.setLength(0);
        this.db = new Database(this.core, this.hasher);
    }

    @Override
    public Long get(long sourceOffset) throws Exception {
        var map = new ReadHashMap(this.db.rootCursor());
        var cursor = map.getCursor(key(sourceOffset));
        return cursor == null ? null : cursor.readUint();
    }

    @Override
    public void put(long sourceOffset, long targetOffset) throws Exception {
        var map = new WriteHashMap(this.db.rootCursor());
        map.put(key(sourceOffset), new Database.Uint(targetOffset));
    }

    private static byte[] key(long offset) {
        // encode the offset directly rather than hashing it, preserving exact keys
        return ByteBuffer.allocate(HASH_SIZE).putLong(HASH_SIZE - Long.BYTES, offset).array();
    }

    @Override
    public void close() throws IOException {
        this.core.close();
    }
}
