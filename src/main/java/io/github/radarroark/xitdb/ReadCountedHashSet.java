package io.github.radarroark.xitdb;

import java.io.IOException;

public class ReadCountedHashSet extends ReadHashSet {
    public ReadCountedHashSet(ReadCursor cursor) throws IOException {
        switch (cursor.resolved().slot().tag()) {
            case NONE, COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                this.cursor = cursor;
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public long count() throws IOException {
        return this.cursor.count();
    }
}
