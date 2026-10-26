package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteCursor extends ReadCursor {
    private final Database.Transaction transaction;
    // writes through this cursor copy any data written before this
    // position, even within the transaction (see freeze)
    private long frozenAt;

    public WriteCursor(SlotPointer slotPtr, Database db) {
        this(slotPtr, db, 0);
    }

    WriteCursor(SlotPointer slotPtr, Database db, long frozenAt) {
        this(slotPtr, db, db.transaction, frozenAt);
    }

    private WriteCursor(SlotPointer slotPtr, Database db, Database.Transaction transaction, long frozenAt) {
        super(slotPtr, db);
        this.transaction = slotPtr.position() == null || Database.isRoot(slotPtr) ? null : transaction;
        this.frozenAt = frozenAt;
    }

    // require the active transaction, its owning thread, and a writable slot
    private void checkWritable() {
        var active = this.db.transaction;
        if (active != null && active.thread != Thread.currentThread()) {
            throw new IllegalStateException("Writer belongs to another thread");
        }
        if (this.transaction != null && this.transaction != active) {
            throw new IllegalStateException("Writer belongs to an expired transaction");
        }
        if (this.slotPtr.position() != null && !Database.isRoot(this.slotPtr) && this.transaction == null) {
            throw new IllegalStateException("Writer was created outside a transaction");
        }
        this.db.checkFrozenSlot(this.slotPtr);
    }

    @Override
    public Slot slot() {
        // rollback may reclaim the storage this slot points to
        if (this.transaction != null && this.transaction.aborted) {
            throw new IllegalStateException("Writer belongs to an aborted transaction");
        }
        return super.slot();
    }

    // reload because an earlier write may have moved what the slot points to
    private void reloadSlot() throws IOException {
        if (this.transaction != null && this.slotPtr.position() != null) {
            this.db.core.seek(this.slotPtr.position());
            var bytes = new byte[Slot.length];
            this.db.core.reader().readFully(bytes);
            this.slotPtr = this.slotPtr.withSlot(Slot.fromBytes(bytes));
        }
    }

    public WriteCursor writePath(Database.PathPart[] path) throws Exception {
        checkWritable();
        // nested top-level writes could commit before the outer transaction ends
        if (this.db.transaction != null && Database.isRoot(this.slotPtr) && path.length > 0) {
            throw new IllegalStateException("Nested top-level writes are not allowed");
        }
        // every write to the root is a transaction
        var startsTransaction = this.db.transaction == null && Database.isRoot(this.slotPtr);
        if (startsTransaction) this.db.transaction = new Database.Transaction();
        try {
            SlotPointer slotPtr;
            try {
                reloadSlot();
                slotPtr = this.db.readSlotPointer(Database.WriteMode.READ_WRITE, path, 0, this.slotPtr, this.frozenAt);
            } catch (Exception | Error e) {
                if (startsTransaction) this.db.transaction.aborted = true;
                // only truncate when the error escapes the outer write.
                // a nested callback's caller may still commit its work.
                if (this.db.txStart == null) {
                    try {
                        this.db.truncate();
                    } catch (Exception e2) {}
                }
                throw e;
            }
            if (this.db.txStart == null) {
                this.db.core.sync();
            }
            reloadSlot();
            var cursor = new WriteCursor(slotPtr, this.db, this.frozenAt);
            cursor.reloadSlot();
            return cursor;
        } finally {
            if (startsTransaction) this.db.transaction = null;
        }
    }

    /**
     * makes writes through this cursor, and cursors made from it afterward,
     * copy what the transaction wrote so far instead of changing it in
     * place. call it after pointing this slot at data still referred to
     * elsewhere, and before wrapping the cursor in a collection, which keeps
     * its own copy. unlike Database.freeze, other cursors keep writing in place.
     */
    public void freeze() throws IOException {
        if (this.db.txStart == null) throw new Database.ExpectedTxStartException();
        this.frozenAt = this.db.core.length();
    }

    public void write(Database.WriteableData data) throws Exception {
        var cursor = writePath(new Database.PathPart[]{
            new Database.WriteData(data)
        });
        this.slotPtr = cursor.slotPtr;
    }

    public void writeIfEmpty(Database.WriteableData data) throws Exception {
        checkWritable();
        var slotPtr = resolved();
        if (slotPtr.slot().empty()) {
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
            new WriteCursor(kvPairCursor.valueCursor.slotPtr, this.db, this.transaction, this.frozenAt),
            new WriteCursor(kvPairCursor.keyCursor.slotPtr, this.db, this.transaction, this.frozenAt),
            kvPairCursor.hash
        );
    }

    public Writer writer() throws IOException {
        checkWritable();
        // bytes are written to the root with write, which runs inside a transaction
        if (Database.isRoot(this.slotPtr)) throw new Database.CursorNotWriteableException();
        if (this.db.txStart == null) throw new Database.ExpectedTxStartException();
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
            checkWritable();
            if (this.size < this.relativePosition) throw new Database.EndOfStreamException();
            var newPosition = this.relativePosition + buffer.length;

            // another allocation may now follow this byte array.
            // extending it would overwrite that allocation.
            if (newPosition > this.size) {
                var end = this.parent.db.core.length();
                if (end != this.startPosition + this.size) throw new Database.UnexpectedWriterPositionException();
            }

            this.parent.db.core.seek(this.startPosition + this.relativePosition);
            var writer = this.parent.db.core.writer();
            writer.write(buffer);
            this.relativePosition = newPosition;
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
            checkWritable();
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

        // validate the parent cursor and reject writes to frozen bytes
        private void checkWritable() {
            this.parent.checkWritable();
            if (this.parent.db.txStart == null) throw new Database.ExpectedTxStartException();
            var active = this.parent.db.transaction;
            if (active != null && active.frozenAt != null && this.slot.value() < active.frozenAt) {
                throw new IllegalStateException("Byte writer points into frozen data");
            }
        }

        public void seek(long position) {
            if (position <= this.size) {
                this.relativePosition = position;
            }
        }
    }

    // iterators don't copy shared nodes, so their cursors must
    // be read-only. this also prevents changes to sorted keys.
    public static class Iterator extends ReadCursor.Iterator {
        public Iterator(WriteCursor cursor) throws IOException {
            super(cursor);
        }

        Iterator(ReadCursor cursor, long size, long index, java.util.Stack<ReadCursor.Iterator.Level> stack) {
            super(cursor, size, index, stack);
        }

        // wrap an already-seeked read iterator for the write-side
        // iteratorFrom/iteratorFromIndex methods.
        static Iterator from(ReadCursor.Iterator inner) {
            return new Iterator(inner.cursor, inner.size, inner.index, inner.stack);
        }

        @Override
        public boolean hasNext() {
            return super.hasNext();
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
