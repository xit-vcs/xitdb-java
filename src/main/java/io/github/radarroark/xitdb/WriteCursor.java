package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteCursor extends ReadCursor {
    public WriteCursor(SlotPointer slotPtr, Database db) {
        super(slotPtr, db);
    }

    public WriteCursor writePath(Database.PathPart[] path) throws Exception {
        var slotPtr = this.db.readSlotPointer(Database.WriteMode.READ_WRITE, path, 0, this.slotPtr);
        if (this.db.txStart == null) {
            this.db.core.sync();
        }
        return new WriteCursor(slotPtr, this.db);
    }

    public void write(Database.WriteableData data) throws Exception {
        var cursor = writePath(new Database.PathPart[]{
            new Database.WriteData(data)
        });
        this.slotPtr = cursor.slotPtr;
    }

    public void writeIfEmpty(Database.WriteableData data) throws Exception {
        if (this.slotPtr.slot().empty()) {
            write(data);
        }
    }

    public static class KeyValuePairCursor extends ReadCursor.KeyValuePairCursor {
        public WriteCursor valueCursor;
        public WriteCursor keyCursor;
        public byte[] hash;

        public KeyValuePairCursor(WriteCursor valueCursor, WriteCursor keyCursor, byte[] hash) {
            super(valueCursor, keyCursor, hash);
            this.valueCursor = valueCursor;
            this.keyCursor = keyCursor;
            this.hash = hash;
        }
    }

    @Override
    public KeyValuePairCursor readKeyValuePair() throws IOException {
        var kvPairCursor = super.readKeyValuePair();
        return new KeyValuePairCursor(
            new WriteCursor(kvPairCursor.valueCursor.slotPtr, this.db),
            new WriteCursor(kvPairCursor.keyCursor.slotPtr, this.db),
            kvPairCursor.hash
        );
    }

    public Writer writer() throws IOException {
        var writer = this.db.core.writer();
        var ptrPos = this.db.core.length();
        this.db.core.seek(ptrPos);
        writer.writeLong(0);
        var startPosition = this.db.core.length();
        return new Writer(this, 0, new Slot(ptrPos, Tag.BYTES), startPosition, 0);
    }

    public static class Writer extends java.io.OutputStream {
        WriteCursor parent;
        long size;
        Slot slot;
        long startPosition;
        long relativePosition;
        public byte[] formatTag;

        public Writer(WriteCursor parent, long size, Slot slot, long startPosition, long relativePosition) {
            this.parent = parent;
            this.size = size;
            this.slot = slot;
            this.startPosition = startPosition;
            this.relativePosition = relativePosition;
        }

        @Override
        public void write(int b) throws IOException {
            var buffer = new byte[1];
            buffer[0] = (byte) (b & 0b1111_1111);
            this.write(buffer);
        }

        @Override
        public void write(byte[] buffer) throws IOException {
            if (this.size < this.relativePosition) throw new Database.EndOfStreamException();
            this.parent.db.core.seek(this.startPosition + this.relativePosition);
            var writer = this.parent.db.core.writer();
            writer.write(buffer);
            this.relativePosition += buffer.length;
            if (this.relativePosition > this.size) {
                this.size = this.relativePosition;
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (b.length == len) {
                this.write(b);
            } else {
                this.write(java.util.Arrays.copyOfRange(b, off, off + len));
            }
        }

        public void finish() throws IOException {
            var writer = this.parent.db.core.writer();

            if (this.formatTag != null) {
                this.slot = this.slot.withFull(true); // byte arrays with format tags must have this set to true
                var formatTagPos = this.parent.db.core.length();
                this.parent.db.core.seek(formatTagPos);
                if (this.startPosition + this.size != formatTagPos) throw new Database.UnexpectedWriterPositionException();
                writer.write(this.formatTag);
            }

            this.parent.db.core.seek(this.slot.value());
            writer.writeLong(this.size);

            if (this.parent.slotPtr.position() == null) throw new Database.CursorNotWriteableException();
            long position = this.parent.slotPtr.position();
            this.parent.db.core.seek(position);
            writer.write(this.slot.toBytes());

            this.parent.slotPtr = this.parent.slotPtr.withSlot(this.slot);
        }

        public void seek(long position) {
            if (position <= this.size) {
                this.relativePosition = position;
            }
        }
    }

    public static class Iterator extends ReadCursor.Iterator {
        public Iterator(WriteCursor cursor) throws IOException {
            super(cursor);
        }

        Iterator(ReadCursor cursor, long size, long index, java.util.Stack<ReadCursor.Iterator.Level> stack) {
            super(cursor, size, index, stack);
        }

        // wrap an already-seeked read iterator so it yields write cursors. backs
        // the write-side iteratorFrom/iteratorFromIndex methods.
        static Iterator from(ReadCursor.Iterator inner) {
            return new Iterator(inner.cursor, inner.size, inner.index, inner.stack);
        }

        @Override
        public boolean hasNext() {
            return super.hasNext();
        }

        @Override
        public WriteCursor next() {
            var readCursor = super.next();
            if (readCursor != null) {
                return new WriteCursor(readCursor.slotPtr, readCursor.db);
            } else {
                return null;
            }
        }

    }

    @Override
    public Iterator iterator() {
        try {
            return new Iterator(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
