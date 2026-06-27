package io.github.radarroark.xitdb;

import java.io.IOException;

public class ReadArrayList implements Slotted, Iterable<ReadCursor> {
    public ReadCursor cursor;

    public ReadArrayList(ReadCursor cursor) {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, ARRAY_LIST -> {
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

    // iterate starting at the given index, seeking straight to it instead of
    // walking from the front. negative indexes count from the end.
    public ReadCursor.Iterator iteratorFrom(long index) throws IOException {
        return ReadCursor.Iterator.initArrayListFromIndex(this.cursor, index);
    }

    public ReadCursor getCursor(long index) throws Exception {
        return this.cursor.readPath(new Database.PathPart[]{
            new Database.ArrayListGet(index)
        });
    }

    public Slot getSlot(long index) throws Exception {
        return this.cursor.readPathSlot(new Database.PathPart[]{
            new Database.ArrayListGet(index)
        });
    }
}
