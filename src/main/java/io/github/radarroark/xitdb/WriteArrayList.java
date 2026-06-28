package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteArrayList extends ReadArrayList {
    public WriteArrayList(WriteCursor cursor) throws Exception {
        super(cursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit()
        }));
    }

    @Override
    public WriteCursor.Iterator iterator() {
        return ((WriteCursor)this.cursor).iterator();
    }

    @Override
    public WriteCursor.Iterator iteratorFrom(long index) throws IOException {
        return WriteCursor.Iterator.from(ReadCursor.Iterator.initArrayListFromIndex(this.cursor, index));
    }

    public void put(long index, Database.WriteableData data) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.ArrayListGet(index),
            new Database.WriteData(data)
        });
    }

    public WriteCursor putCursor(long index) throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.ArrayListGet(index),
        });
    }

    public void append(Database.WriteableData data) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.ArrayListAppend(),
            new Database.WriteData(data),
        });
    }

    public WriteCursor appendCursor() throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.ArrayListAppend(),
        });
    }

    public void appendContext(Database.WriteableData data, Database.ContextFunction fn) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.ArrayListAppend(),
            new Database.WriteData(data),
            new Database.Context(fn)
        });
    }

    public void slice(long size) throws Exception {
        ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.ArrayListSlice(size)
        });
    }
}
