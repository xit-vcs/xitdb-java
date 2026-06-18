package io.github.radarroark.xitdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Stack;

public class ReadCursor implements Slotted, Iterable<ReadCursor> {
    public SlotPointer slotPtr;
    public Database db;

    public ReadCursor(SlotPointer slotPtr, Database db) {
        this.slotPtr = slotPtr;
        this.db = db;
    }

    @Override
    public Slot slot() {
        return this.slotPtr.slot();
    }

    public ReadCursor readPath(Database.PathPart[] path) throws Exception {
        try {
            var slotPtr = this.db.readSlotPointer(Database.WriteMode.READ_ONLY, path, 0, this.slotPtr);
            return new ReadCursor(slotPtr, this.db);
        } catch (Database.KeyNotFoundException e) {
            return null;
        }
    }

    public Slot readPathSlot(Database.PathPart[] path) throws Exception {
        try {
            var slotPtr = this.db.readSlotPointer(Database.WriteMode.READ_ONLY, path, 0, this.slotPtr);
            if (!slotPtr.slot().empty()) {
                return slotPtr.slot();
            } else {
                return null;
            }
        } catch (Database.KeyNotFoundException e) {
            return null;
        }
    }

    public long readUint() {
        if (this.slotPtr.slot().tag() != Tag.UINT) {
            throw new Database.UnexpectedTagException();
        }
        if (this.slotPtr.slot().value() < 0) throw new Database.ExpectedUnsignedLongException();
        return this.slotPtr.slot().value();
    }

    public long readInt() {
        if (this.slotPtr.slot().tag() != Tag.INT) {
            throw new Database.UnexpectedTagException();
        }
        return this.slotPtr.slot().value();
    }

    public double readFloat() {
        if (this.slotPtr.slot().tag() != Tag.FLOAT) {
            throw new Database.UnexpectedTagException();
        }
        var buffer = ByteBuffer.allocate(8);
        buffer.putLong(this.slotPtr.slot().value());
        buffer.position(0);
        return buffer.getDouble();
    }

    public byte[] readBytes(Long maxSizeMaybe) throws IOException {
        return readBytesObject(maxSizeMaybe).value();
    }

    public Database.Bytes readBytesObject(Long maxSizeMaybe) throws IOException {
        var reader = this.db.core.reader();

        switch (this.slotPtr.slot().tag()) {
            case NONE -> {
                return new Database.Bytes(new byte[0]);
            }
            case BYTES -> {
                this.db.core.seek(this.slotPtr.slot().value());
                var valueSize = reader.readLong();

                if (maxSizeMaybe != null && valueSize > maxSizeMaybe) {
                    throw new Database.StreamTooLongException();
                }

                var startPosition = this.db.core.position();

                var value = new byte[(int)valueSize];
                reader.readFully(value);

                byte[] formatTag = null;
                if (this.slotPtr.slot().full()) {
                    this.db.core.seek(startPosition + valueSize);
                    formatTag = new byte[2];
                    reader.readFully(formatTag);
                }

                return new Database.Bytes(value, formatTag);
            }
            case SHORT_BYTES -> {
                var buffer = ByteBuffer.allocate(8);
                buffer.putLong(this.slotPtr.slot().value());
                var bytes = buffer.array();

                var totalSize = this.slotPtr.slot().full() ? bytes.length - 2 : bytes.length;

                var valueSize = 0;
                for (byte b : bytes) {
                    if (b == 0 || valueSize == totalSize) break;
                    valueSize += 1;
                }

                if (maxSizeMaybe != null && valueSize > maxSizeMaybe) {
                    throw new Database.StreamTooLongException();
                }

                byte[] formatTag = null;
                if (this.slotPtr.slot().full()) {
                    formatTag = Arrays.copyOfRange(bytes, totalSize, bytes.length);
                }

                return new Database.Bytes(Arrays.copyOfRange(bytes, 0, valueSize), formatTag);
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public static class KeyValuePairCursor {
        public ReadCursor valueCursor;
        public ReadCursor keyCursor;
        public byte[] hash;

        public KeyValuePairCursor(ReadCursor valueCursor, ReadCursor keyCursor, byte[] hash) {
            this.valueCursor = valueCursor;
            this.keyCursor = keyCursor;
            this.hash = hash;
        }
    }

    public KeyValuePairCursor readKeyValuePair() throws IOException {
        var reader = this.db.core.reader();

        if (this.slotPtr.slot().tag() != Tag.KV_PAIR) {
            throw new Database.UnexpectedTagException();
        }

        this.db.core.seek(this.slotPtr.slot().value());
        var kvPairBytes = new byte[Database.KeyValuePair.length(this.db.header.hashSize())];
        reader.readFully(kvPairBytes);
        var kvPair = Database.KeyValuePair.fromBytes(kvPairBytes, this.db.header.hashSize());

        var hashPos = this.slotPtr.slot().value();
        var keySlotPos = hashPos + this.db.header.hashSize();
        var valueSlotPos = keySlotPos + Slot.length;

        return new KeyValuePairCursor(
            new ReadCursor(new SlotPointer(valueSlotPos, kvPair.valueSlot()), this.db),
            new ReadCursor(new SlotPointer(keySlotPos, kvPair.keySlot()), this.db),
            kvPair.hash()
        );
    }

    public Reader reader() throws IOException {
        var reader = this.db.core.reader();

        switch (this.slotPtr.slot().tag()) {
            case BYTES -> {
                this.db.core.seek(this.slotPtr.slot().value());
                var size = reader.readLong();
                var startPosition = this.db.core.position();
                return new Reader(this, size, startPosition, 0);
            }
            case SHORT_BYTES -> {
                var buffer = ByteBuffer.allocate(8);
                buffer.putLong(this.slotPtr.slot().value());
                var bytes = buffer.array();

                var totalSize = this.slotPtr.slot().full() ? bytes.length - 2 : bytes.length;

                var valueSize = 0;
                for (byte b : bytes) {
                    if (b == 0 || valueSize == totalSize) break;
                    valueSize += 1;
                }

                // add one to get past the tag byte
                var startPosition = this.slotPtr.position() + 1;
                return new Reader(this, valueSize, startPosition, 0);
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public long count() throws IOException {
        var reader = this.db.core.reader();
        switch (this.slotPtr.slot().tag()) {
            case NONE -> {
                return 0;
            }
            case ARRAY_LIST -> {
                this.db.core.seek(this.slotPtr.slot().value());
                var headerBytes = new byte[Database.ArrayListHeader.length];
                reader.readFully(headerBytes);
                var header = Database.ArrayListHeader.fromBytes(headerBytes);
                return header.size();
            }
            case LINKED_ARRAY_LIST -> {
                this.db.core.seek(this.slotPtr.slot().value());
                var headerBytes = new byte[Database.BTreeHeader.length];
                reader.readFully(headerBytes);
                var header = Database.BTreeHeader.fromBytes(headerBytes);
                return header.size();
            }
            case BYTES -> {
                this.db.core.seek(this.slotPtr.slot().value());
                return reader.readLong();
            }
            case SHORT_BYTES -> {
                var buffer = ByteBuffer.allocate(8);
                buffer.putLong(this.slotPtr.slot().value());
                var bytes = buffer.array();

                var totalSize = this.slotPtr.slot().full() ? bytes.length - 2 : bytes.length;

                var size = 0;
                for (byte b : bytes) {
                    if (b == 0 || size == totalSize) break;
                    size += 1;
                }
                return size;
            }
            case COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                this.db.core.seek(this.slotPtr.slot().value());
                return reader.readLong();
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public static class Reader extends java.io.InputStream {
        ReadCursor parent;
        long size;
        long startPosition;
        long relativePosition;

        public Reader(ReadCursor parent, long size, long startPosition, long relativePosition) {
            this.parent = parent;
            this.size = size;
            this.startPosition = startPosition;
            this.relativePosition = relativePosition;
        }

        @Override
        public int read() throws IOException {
            try {
                return this.readByte();
            } catch (Database.EndOfStreamException e) {
                return -1;
            }
        }

        @Override
        public int read(byte[] buffer) throws IOException {
            if (this.size < this.relativePosition) throw new Database.EndOfStreamException();
            this.parent.db.core.seek(this.startPosition + this.relativePosition);
            var readSize = Math.min(buffer.length, (int) (this.size - this.relativePosition));
            if (readSize == 0) return -1;
            var reader = this.parent.db.core.reader();
            reader.readFully(buffer, 0, readSize);
            this.relativePosition += readSize;
            return readSize;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (b.length == len) {
                return this.read(b);
            } else {
                var buffer = new byte[len];
                var readSize = this.read(buffer);
                if (readSize == -1) return -1;
                System.arraycopy(buffer, 0, b, off, readSize);
                return readSize;
            }
        }

        public void readFully(byte[] buffer) throws IOException {
            if (this.size < this.relativePosition || this.size - this.relativePosition < buffer.length) throw new Database.EndOfStreamException();
            this.parent.db.core.seek(this.startPosition + this.relativePosition);
            var reader = this.parent.db.core.reader();
            reader.readFully(buffer);
            this.relativePosition += buffer.length;
        }

        public byte readByte() throws IOException {
            var bytes = new byte[1];
            this.readFully(bytes);
            return bytes[0];
        }

        public short readShort() throws IOException {
            var readSize = 2;
            var bytes = new byte[readSize];
            this.readFully(bytes);
            var buffer = ByteBuffer.allocate(readSize);
            buffer.put(bytes);
            buffer.position(0);
            return buffer.getShort();
        }

        public int readInt() throws IOException {
            var readSize = 4;
            var bytes = new byte[readSize];
            this.readFully(bytes);
            var buffer = ByteBuffer.allocate(readSize);
            buffer.put(bytes);
            buffer.position(0);
            return buffer.getInt();
        }

        public long readLong() throws IOException {
            var readSize = 8;
            var bytes = new byte[readSize];
            this.readFully(bytes);
            var buffer = ByteBuffer.allocate(readSize);
            buffer.put(bytes);
            buffer.position(0);
            return buffer.getLong();
        }

        public void seek(long position) {
            if (position > this.size) {
                throw new Database.InvalidOffsetException();
            }
            this.relativePosition = position;
        }
    }

    public static class Iterator implements java.util.Iterator<ReadCursor> {
        ReadCursor cursor;
        long size;
        long index;
        private Stack<Level> stack;
        private ReadCursor nextCursorMaybe = null; // only used when iterating over hash maps

        public static class Level {
            long position;
            Slot[] block;
            byte index;

            public Level(long position, Slot[] block, byte index) {
                this.position = position;
                this.block = block;
                this.index = index;
            }
        }

        public Iterator(ReadCursor cursor) throws IOException {
            this.cursor = cursor;
            switch (cursor.slotPtr.slot().tag()) {
                case NONE -> {
                    this.size = 0;
                    this.index = 0;
                    this.stack = new Stack<Level>();
                }
                case ARRAY_LIST -> {
                    var position = cursor.slotPtr.slot().value();
                    cursor.db.core.seek(position);
                    var reader = cursor.db.core.reader();
                    var headerBytes = new byte[Database.ArrayListHeader.length];
                    reader.readFully(headerBytes);
                    var header = Database.ArrayListHeader.fromBytes(headerBytes);
                    this.size = cursor.count();
                    this.index = 0;
                    this.stack = initStack(cursor, header.ptr());
                }
                case LINKED_ARRAY_LIST -> {
                    // backed by a b-tree: read the header, then walk from the root
                    // node's value/child slots (skipping its kind+num header)
                    var position = cursor.slotPtr.slot().value();
                    cursor.db.core.seek(position);
                    var reader = cursor.db.core.reader();
                    var headerBytes = new byte[Database.BTreeHeader.length];
                    reader.readFully(headerBytes);
                    var header = Database.BTreeHeader.fromBytes(headerBytes);
                    this.size = cursor.count();
                    this.index = 0;
                    this.stack = initStack(cursor, header.rootPtr() + Database.BTREE_NODE_HEADER_SIZE);
                }
                case HASH_MAP, HASH_SET -> {
                    this.size = 0;
                    this.index = 0;
                    this.stack = initStack(cursor, cursor.slotPtr.slot().value());
                }
                case COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                    this.size = 0;
                    this.index = 0;
                    this.stack = initStack(cursor, cursor.slotPtr.slot().value() + 8);
                }
                default -> throw new Database.UnexpectedTagException();
            }
        }

        @Override
        public boolean hasNext() {
            return switch (this.cursor.slotPtr.slot().tag()) {
                case NONE -> false;
                case ARRAY_LIST -> this.index < this.size;
                case LINKED_ARRAY_LIST -> this.index < this.size;
                case HASH_MAP, HASH_SET, COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                    // the only way to determine if there's another value in the
                    // hash map is to try to retrieve it, so we store it in a
                    // field and then read from that field when next() is called.
                    if (this.nextCursorMaybe == null) {
                        try {
                            this.nextCursorMaybe = nextInternal(0);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    yield this.nextCursorMaybe != null;
                }
                default -> false;
            };
        }

        @Override
        public ReadCursor next() {
            try {
                switch (this.cursor.slotPtr.slot().tag()) {
                    case NONE -> {
                        return null;
                    }
                    case ARRAY_LIST -> {
                        if (!hasNext()) return null;
                        this.index += 1;
                        return nextInternal(0);
                    }
                    case LINKED_ARRAY_LIST -> {
                        if (!hasNext()) return null;
                        this.index += 1;
                        // b-tree nodes have a kind+num header before their slots,
                        // so child pointers are offset by BTREE_NODE_HEADER_SIZE
                        return nextInternal(Database.BTREE_NODE_HEADER_SIZE);
                    }
                    case HASH_MAP, HASH_SET, COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                        if (this.nextCursorMaybe != null) {
                            var nextCursor = this.nextCursorMaybe;
                            this.nextCursorMaybe = null;
                            return nextCursor;
                        } else {
                            return nextInternal(0);
                        }
                    }
                    default -> throw new Database.UnexpectedTagException();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        // read a 16-slot index block (the iterable structures all use 9-byte slots
        // in their index/node blocks)
        private static Slot[] readSlotBlock(ReadCursor cursor, long position) throws IOException {
            cursor.db.core.seek(position);
            var reader = cursor.db.core.reader();
            var indexBlockBytes = new byte[Database.INDEX_BLOCK_SIZE];
            reader.readFully(indexBlockBytes);
            var indexBlock = new Slot[Database.SLOT_COUNT];
            var buffer = ByteBuffer.wrap(indexBlockBytes);
            for (int i = 0; i < indexBlock.length; i++) {
                var slotBytes = new byte[Slot.length];
                buffer.get(slotBytes);
                indexBlock[i] = Slot.fromBytes(slotBytes);
            }
            return indexBlock;
        }

        private static Stack<Level> initStack(ReadCursor cursor, long position) throws IOException {
            var stack = new Stack<Level>();
            stack.add(new Level(position, readSlotBlock(cursor, position), (byte)0));
            return stack;
        }

        private ReadCursor nextInternal(long nodeOffset) throws IOException {
            while (!this.stack.empty()) {
                var level = this.stack.peek();
                if (level.index == level.block.length) {
                    this.stack.pop();
                    if (!this.stack.empty()) {
                        this.stack.peek().index += 1;
                    }
                    continue;
                } else {
                    var nextSlot = level.block[level.index];
                    if (nextSlot.tag() == Tag.INDEX) {
                        // nodeOffset skips a b-tree node's kind+num header
                        var nextPos = nextSlot.value() + nodeOffset;
                        stack.add(new Level(nextPos, readSlotBlock(this.cursor, nextPos), (byte)0));
                        continue;
                    } else {
                        this.stack.peek().index += 1;
                        // normally a slot that is .none should be skipped because it doesn't
                        // have a value, but if it's set to full, then it is actually a valid
                        // item that should be returned.
                        if (!nextSlot.empty()) {
                            var position = level.position + (level.index * Slot.length);
                            return new ReadCursor(new SlotPointer(position, nextSlot), this.cursor.db);
                        } else {
                            continue;
                        }
                    }
                }
            }
            return null;
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
