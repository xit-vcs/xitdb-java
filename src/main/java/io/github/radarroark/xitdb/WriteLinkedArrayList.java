package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteLinkedArrayList extends ReadLinkedArrayList {
    public WriteLinkedArrayList(WriteCursor cursor) throws Exception {
        super(cursor.writePath(new Database.PathPart[]{
            new Database.LinkedArrayListInit()
        }));
    }

    @Override
    public WriteCursor.Iterator iterator() {
        return ((WriteCursor)this.cursor).iterator();
    }

    @Override
    public WriteCursor.Iterator iteratorFrom(long index) throws IOException {
        return WriteCursor.Iterator.from(ReadCursor.Iterator.initLinkedArrayListFromIndex(this.cursor, index));
    }

    public void put(long index, Database.WriteableData data) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListGet(index),
            new Database.WriteData(data)
        });
    }

    public WriteCursor putCursor(long index) throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListGet(index),
        });
    }

    public void append(Database.WriteableData data) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListAppend(),
            new Database.WriteData(data),
        });
    }

    public WriteCursor appendCursor() throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListAppend(),
        });
    }

    public void slice(long offset, long size) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListSlice(offset, size)
        });
    }

    public void concat(Slot list) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListConcat(list)
        });
    }

    public void insert(long index, Database.WriteableData data) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListInsert(index),
            new Database.WriteData(data)
        });
    }

    public WriteCursor insertCursor(long index) throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListInsert(index),
        });
    }

    public void remove(long index) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.LinkedArrayListRemove(index),
        });
    }
}
