package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteCountedHashMap extends WriteHashMap {
    public WriteCountedHashMap(WriteCursor cursor) throws Exception {
        switch (cursor.resolved().slot().tag()) {
            case NONE, COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                this.cursor = cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapInit(true, false)
                });
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public long count() throws IOException {
        return this.cursor.count();
    }
}
