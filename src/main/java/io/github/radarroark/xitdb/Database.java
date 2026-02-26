package io.github.radarroark.xitdb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class Database {
    public Core core;
    public MessageDigest md;
    public Header header;
    public Long txStart;

    public static final short VERSION = 0;
    public static final byte[] MAGIC_NUMBER = new byte[]{'x', 'i', 't'};
    public static final int DATABASE_START = Header.length;
    public static final int BIT_COUNT = 4;
    public static final int SLOT_COUNT = 1 << BIT_COUNT;
    public static final long MASK = SLOT_COUNT - 1;
    public static final BigInteger BIG_MASK = BigInteger.valueOf(MASK);
    public static final int INDEX_BLOCK_SIZE = Slot.length * SLOT_COUNT;
    public static final int LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE = LinkedArrayListSlot.length * SLOT_COUNT;
    public static final int MAX_BRANCH_LENGTH = 16;

    public static enum WriteMode {
        READ_ONLY,
        READ_WRITE
    }

    // init

    public Database(Core core, Hasher hasher) throws IOException {
        this.core = core;
        this.md = hasher.md();

        core.seek(0);
        if (core.length() == 0) {
            this.header = new Header(hasher.id(), (short)hasher.md().getDigestLength(), VERSION, Tag.NONE, MAGIC_NUMBER);
            this.header.write(core);
            this.core.flush();
        } else {
            this.header = Header.read(core);
            this.header.validate();
            if (this.header.hashSize() != hasher.md().getDigestLength()) {
                throw new InvalidHashSizeException();
            }
            truncate();
        }

        this.txStart = null;
    }

    public WriteCursor rootCursor() throws IOException {
        // if the header tag is none, try re-reading it.
        // this may be necessary if the database was initialized on a different thread.
        if (this.header.tag() == Tag.NONE) {
            core.seek(0);
            this.header = Header.read(core);
        }
        return new WriteCursor(new SlotPointer(null, new Slot(DATABASE_START, this.header.tag)), this);
    }

    public void freeze() throws IOException {
        if (this.txStart != null) {
            this.txStart = this.core.length();
        } else {
            throw new ExpectedTxStartException();
        }
    }

    public Database compact(Core targetCore) throws Exception {
        var offsetMap = new HashMap<Long, Long>();
        var hasher = new Hasher(this.md, this.header.hashId());
        var target = new Database(targetCore, hasher);

        if (this.header.tag() == Tag.NONE) return target;
        if (this.header.tag() != Tag.ARRAY_LIST) throw new UnexpectedTagException();

        // read source's top-level ArrayListHeader
        this.core.seek(DATABASE_START);
        var sourceReader = this.core.reader();
        var headerBytes = new byte[ArrayListHeader.length];
        sourceReader.readFully(headerBytes);
        var sourceHeader = ArrayListHeader.fromBytes(headerBytes);

        if (sourceHeader.size() == 0) return target;

        // read the last moment's slot
        var lastKey = sourceHeader.size() - 1;
        var shift = (byte)(lastKey < SLOT_COUNT ? 0 : (int)(Math.log(lastKey) / Math.log(SLOT_COUNT)));
        var lastSlotPtr = this.readArrayListSlot(sourceHeader.ptr(), lastKey, shift, WriteMode.READ_ONLY, true);
        var momentSlot = lastSlotPtr.slot();

        // write TopLevelArrayListHeader + root index block to target
        var targetWriter = target.core.writer();
        target.core.seek(DATABASE_START);
        var targetArrayListPtr = DATABASE_START + TopLevelArrayListHeader.length;
        targetWriter.write(new TopLevelArrayListHeader(
            0,
            new ArrayListHeader(targetArrayListPtr, 1)
        ).toBytes());
        targetWriter.write(new byte[INDEX_BLOCK_SIZE]);

        // recursively remap the moment slot
        var remappedMoment = remapSlot(this.core, target.core, this.header.hashSize(), offsetMap, momentSlot);

        // write remapped moment slot into position 0 of target's root index block
        target.core.seek(targetArrayListPtr);
        targetWriter.write(remappedMoment.toBytes());

        // update target's DatabaseHeader tag
        target.header = target.header.withTag(Tag.ARRAY_LIST);
        target.core.seek(0);
        target.header.write(target.core);

        // flush, update file_size, flush again
        target.core.flush();
        var fileSize = target.core.length();
        target.core.seek(DATABASE_START + ArrayListHeader.length);
        targetWriter.writeLong(fileSize);
        target.core.flush();

        return target;
    }

    // private

    private void truncate() throws IOException {
        if (this.header.tag() != Tag.ARRAY_LIST) return;

        var rootCursor = rootCursor();
        var listSize = rootCursor.count();

        if (listSize == 0) return;

        this.core.seek(DATABASE_START + ArrayListHeader.length);
        var reader = this.core.reader();
        var headerFileSize = reader.readLong();

        if (headerFileSize == 0) return;

        var fileSize = this.core.length();

        if (fileSize == headerFileSize) return;

        // ignore error because the file may be open in read-only mode
        try {
            this.core.setLength(headerFileSize);
        } catch (IOException e) {}
    }

    private byte[] checkHash(byte[] hash) {
        if (hash.length != this.header.hashSize()) {
            throw new InvalidHashSizeException();
        }
        return hash;
    }

    private byte[] checkHash(HashMapGetTarget target) {
        if (target instanceof HashMapGetKVPair kvPairTarget) {
            return checkHash(kvPairTarget.hash());
        } else if (target instanceof HashMapGetKey keyTarget) {
            return checkHash(keyTarget.hash());
        } else if (target instanceof HashMapGetValue valueTarget) {
            return checkHash(valueTarget.hash());
        } else {
            throw new IllegalArgumentException();
        }
    }

    private static long checkLong(long n) {
        if (n < 0) {
            throw new ExpectedUnsignedLongException();
        }
        return n;
    }

    protected SlotPointer readSlotPointer(WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
        if (pathI == path.length) {
            if (writeMode == WriteMode.READ_ONLY && slotPtr.slot().tag() == Tag.NONE) {
                throw new KeyNotFoundException();
            }
            return slotPtr;
        }
        var part = path[pathI];

        var isTopLevel = slotPtr.slot().value() == DATABASE_START;

        var isTxStart = isTopLevel && this.header.tag == Tag.ARRAY_LIST && this.txStart == null;
        if (isTxStart) {
            this.txStart = this.core.length();
        }

        try {
            return part.readSlotPointer(this, isTopLevel, writeMode, path, pathI, slotPtr);
        } finally {
            if (isTxStart) {
                this.txStart = null;
            }
        }
    }

    // records

    public static record Header (
        int hashId,
        short hashSize,
        short version,
        Tag tag,
        byte[] magicNumber
    ) {
        public static int length = 12;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.magicNumber);
            buffer.put((byte)this.tag.ordinal());
            buffer.putShort(this.version);
            buffer.putShort(this.hashSize);
            buffer.putInt(this.hashId);
            return buffer.array();
        }

        public static Header read(Core core) throws IOException {
            var reader = core.reader();
            var magicNumber = new byte[3];
            reader.readFully(magicNumber);
            var tag = Tag.valueOf(reader.readByte() & 0b0111_1111);
            var version = reader.readShort();
            var hashSize = reader.readShort();
            var hashId = reader.readInt();
            return new Header(hashId, hashSize, version, tag, magicNumber);
        }

        public void write(Core core) throws IOException {
            var writer = core.writer();
            writer.write(this.toBytes());
        }

        public void validate() {
            if (!Arrays.equals(this.magicNumber, MAGIC_NUMBER)) {
                throw new InvalidDatabaseException();
            }
            if (this.version > VERSION) {
                throw new InvalidVersionException();
            }
        }

        public Header withTag(Tag tag) {
            return new Header(this.hashId, this.hashSize, this.version, tag, this.magicNumber);
        }
    }

    public static record ArrayListHeader(long ptr, long size) {
        public static int length = 16;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.putLong(this.size);
            buffer.putLong(this.ptr);
            return buffer.array();
        }

        public static ArrayListHeader fromBytes(byte[] bytes) {
            var buffer = ByteBuffer.wrap(bytes);
            var size = checkLong(buffer.getLong());
            var ptr = checkLong(buffer.getLong());
            return new ArrayListHeader(ptr, size);
        }

        public ArrayListHeader withPtr(long ptr) {
            return new ArrayListHeader(ptr, this.size);
        }
    }

    public static record TopLevelArrayListHeader(long fileSize, ArrayListHeader parent) {
        public static int length = 8 + ArrayListHeader.length;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.parent.toBytes());
            buffer.putLong(this.fileSize);
            return buffer.array();
        }
    }

    public static record LinkedArrayListHeader(byte shift, long ptr, long size) {
        public static int length = 17;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.putLong(this.size);
            buffer.putLong(this.ptr);
            buffer.put((byte) (this.shift & 0b0011_1111));
            return buffer.array();
        }

        public static LinkedArrayListHeader fromBytes(byte[] bytes) {
            var buffer = ByteBuffer.wrap(bytes);
            var size = checkLong(buffer.getLong());
            var ptr = checkLong(buffer.getLong());
            var shift = (byte) (buffer.get() & 0b0011_1111);
            return new LinkedArrayListHeader(shift, ptr, size);
        }

        public LinkedArrayListHeader withPtr(long ptr) {
            return new LinkedArrayListHeader(this.shift, ptr, this.size);
        }
    }

    public static record KeyValuePair(Slot valueSlot, Slot keySlot, byte[] hash) {
        public static int length(int hashSize) {
            return hashSize + Slot.length * 2;
        }

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length(hash.length));
            buffer.put(hash);
            buffer.put(keySlot.toBytes());
            buffer.put(valueSlot.toBytes());
            return buffer.array();
        }

        public static KeyValuePair fromBytes(byte[] bytes, int hashSize) {
            var buffer = ByteBuffer.wrap(bytes);
            var hash = new byte[hashSize];
            buffer.get(hash);
            var keySlotBytes = new byte[Slot.length];
            buffer.get(keySlotBytes);
            var keySlot = Slot.fromBytes(keySlotBytes);
            var valueSlotBytes = new byte[Slot.length];
            buffer.get(valueSlotBytes);
            var valueSlot = Slot.fromBytes(valueSlotBytes);
            return new KeyValuePair(valueSlot, keySlot, hash);
        }
    }

    public static sealed interface PathPart permits ArrayListInit, ArrayListGet, ArrayListAppend, ArrayListSlice, LinkedArrayListInit, LinkedArrayListGet, LinkedArrayListAppend, LinkedArrayListSlice, LinkedArrayListConcat, LinkedArrayListInsert, LinkedArrayListRemove, HashMapInit, HashMapGet, HashMapRemove, WriteData, Context {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception;
    }

    public static record ArrayListInit() implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (isTopLevel) {
                var writer = db.core.writer();

                // if the top level array list hasn't been initialized
                if (db.header.tag == Tag.NONE) {
                    // write the array list header
                    db.core.seek(DATABASE_START);
                    var arrayListPtr = DATABASE_START + TopLevelArrayListHeader.length;
                    writer.write((new TopLevelArrayListHeader(
                        0,
                        new ArrayListHeader(arrayListPtr, 0))
                    ).toBytes());

                    // write the first block
                    writer.write(new byte[INDEX_BLOCK_SIZE]);

                    // update db header
                    db.core.seek(0);
                    db.header = db.header.withTag(Tag.ARRAY_LIST);
                    writer.write(db.header.toBytes());
                }

                var nextSlotPtr = slotPtr.withSlot(slotPtr.slot().withTag(Tag.ARRAY_LIST));
                return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
            }

            if (slotPtr.position() == null) throw new CursorNotWriteableException();
            long position = slotPtr.position();

            switch (slotPtr.slot().tag()) {
                case NONE -> {
                    // if slot was empty, insert the new list
                    var writer = db.core.writer();
                    var arrayListStart = db.core.length();
                    db.core.seek(arrayListStart);
                    var arrayListPtr = arrayListStart + ArrayListHeader.length;
                    writer.write(new ArrayListHeader(
                        arrayListPtr,
                        0
                    ).toBytes());
                    writer.write(new byte[INDEX_BLOCK_SIZE]);
                    // make slot point to list
                    var nextSlotPtr = new SlotPointer(position, new Slot(arrayListStart, Tag.ARRAY_LIST));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                case ARRAY_LIST -> {
                    var reader = db.core.reader();
                    var writer = db.core.writer();

                    var arrayListStart = slotPtr.slot().value();

                    // copy it to the end unless it was made in this transaction
                    if (db.txStart != null) {
                        if (arrayListStart < db.txStart) {
                            // read existing block
                            db.core.seek(arrayListStart);
                            var headerBytes = new byte[ArrayListHeader.length];
                            reader.readFully(headerBytes);
                            var header = ArrayListHeader.fromBytes(headerBytes);
                            db.core.seek(header.ptr);
                            var arrayListIndexBlock = new byte[INDEX_BLOCK_SIZE];
                            reader.readFully(arrayListIndexBlock);
                            // copy to the end
                            arrayListStart = db.core.length();
                            db.core.seek(arrayListStart);
                            var nextArrayListPtr = arrayListStart + ArrayListHeader.length;
                            header = header.withPtr(nextArrayListPtr);
                            writer.write(header.toBytes());
                            writer.write(arrayListIndexBlock);
                        }
                    } else if (db.header.tag() == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }

                    // make slot point to list
                    var nextSlotPtr = new SlotPointer(position, new Slot(arrayListStart, Tag.ARRAY_LIST));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                default -> throw new UnexpectedTagException();
            }
        }
    }

    public static record ArrayListGet(long index) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            var tag = isTopLevel ? db.header.tag : slotPtr.slot().tag();
            switch (tag) {
                case NONE -> throw new KeyNotFoundException();
                case ARRAY_LIST -> {}
                default -> throw new UnexpectedTagException();
            }

            var nextArrayListStart = slotPtr.slot().value();
            var index = this.index();

            db.core.seek(nextArrayListStart);
            var reader = db.core.reader();
            var headerBytes = new byte[ArrayListHeader.length];
            reader.readFully(headerBytes);
            var header = ArrayListHeader.fromBytes(headerBytes);
            if (index >= header.size || index < -header.size) {
                throw new KeyNotFoundException();
            }

            var key = index < 0 ? header.size - Math.abs(index) : index;
            var lastKey = header.size - 1;
            var shift = (byte) (lastKey < SLOT_COUNT ? 0 : Math.log(lastKey) / Math.log(SLOT_COUNT));
            var finalSlotPtr = db.readArrayListSlot(header.ptr, key, shift, writeMode, isTopLevel);

            return db.readSlotPointer(writeMode, path, pathI + 1, finalSlotPtr);
        }
    }

    public static record ArrayListAppend() implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            var tag = isTopLevel ? db.header.tag : slotPtr.slot().tag();
            if (tag != Tag.ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            var nextArrayListStart = slotPtr.slot().value();

            // read header
            db.core.seek(nextArrayListStart);
            var headerBytes = new byte[ArrayListHeader.length];
            reader.readFully(headerBytes);
            var origHeader = ArrayListHeader.fromBytes(headerBytes);

            // append
            var appendResult = db.readArrayListSlotAppend(origHeader, writeMode, isTopLevel);
            var finalSlotPtr = db.readSlotPointer(writeMode, path, pathI + 1, appendResult.slotPtr());

            var writer = db.core.writer();
            if (isTopLevel) {
                // it is very important that we flush before updating the header,
                // because updating the header is what completes the transaction
                db.core.flush();

                var fileSize = db.core.length();
                var header = new TopLevelArrayListHeader(fileSize, appendResult.header);

                // update header
                db.core.seek(nextArrayListStart);
                writer.write(header.toBytes());
            } else {
                // update header
                db.core.seek(nextArrayListStart);
                writer.write(appendResult.header().toBytes());
            }

            return finalSlotPtr;
        }
    }

    public static record ArrayListSlice(long size) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            var nextArrayListStart = slotPtr.slot().value();

            // read header
            db.core.seek(nextArrayListStart);
            var headerBytes = new byte[ArrayListHeader.length];
            reader.readFully(headerBytes);
            var origHeader = ArrayListHeader.fromBytes(headerBytes);

            // slice
            var sliceHeader = db.readArrayListSlice(origHeader, this.size());
            var finalSlotPtr = db.readSlotPointer(writeMode, path, pathI + 1, slotPtr);

            // update header
            var writer = db.core.writer();
            db.core.seek(nextArrayListStart);
            writer.write(sliceHeader.toBytes());

            return finalSlotPtr;
        }
    }

    public static record LinkedArrayListInit() implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (isTopLevel) throw new InvalidTopLevelTypeException();

            if (slotPtr.position() == null) throw new CursorNotWriteableException();
            long position = slotPtr.position();

            switch (slotPtr.slot().tag()) {
                case NONE -> {
                    // if slot was empty, insert the new list
                    var writer = db.core.writer();
                    var arrayListStart = db.core.length();
                    db.core.seek(arrayListStart);
                    var arrayListPtr = arrayListStart + LinkedArrayListHeader.length;
                    writer.write(new LinkedArrayListHeader(
                        (byte)0,
                        arrayListPtr,
                        0
                    ).toBytes());
                    writer.write(new byte[LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE]);
                    // make slot point to list
                    var nextSlotPtr = new SlotPointer(position, new Slot(arrayListStart, Tag.LINKED_ARRAY_LIST));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                case LINKED_ARRAY_LIST -> {
                    var reader = db.core.reader();
                    var writer = db.core.writer();

                    var arrayListStart = slotPtr.slot().value();

                    // copy it to the end unless it was made in this transaction
                    if (db.txStart != null) {
                        if (arrayListStart < db.txStart) {
                            // read existing block
                            db.core.seek(arrayListStart);
                            var headerBytes = new byte[LinkedArrayListHeader.length];
                            reader.readFully(headerBytes);
                            var header = LinkedArrayListHeader.fromBytes(headerBytes);
                            db.core.seek(header.ptr);
                            var arrayListIndexBlock = new byte[LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE];
                            reader.readFully(arrayListIndexBlock);
                            // copy to the end
                            arrayListStart = db.core.length();
                            db.core.seek(arrayListStart);
                            var nextArrayListPtr = arrayListStart + LinkedArrayListHeader.length;
                            header = header.withPtr(nextArrayListPtr);
                            writer.write(header.toBytes());
                            writer.write(arrayListIndexBlock);
                        }
                    } else if (db.header.tag() == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }

                    // make slot point to list
                    var nextSlotPtr = new SlotPointer(position, new Slot(arrayListStart, Tag.LINKED_ARRAY_LIST));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                default -> throw new UnexpectedTagException();
            }
        }
    }

    public static record LinkedArrayListGet(long index) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case LINKED_ARRAY_LIST -> {}
                default -> throw new UnexpectedTagException();
            }

            var index = this.index();

            db.core.seek(slotPtr.slot().value());
            var reader = db.core.reader();
            var headerBytes = new byte[LinkedArrayListHeader.length];
            reader.readFully(headerBytes);
            var header = LinkedArrayListHeader.fromBytes(headerBytes);
            if (index >= header.size() || index < -header.size()) {
                throw new KeyNotFoundException();
            }

            var key = index < 0 ? header.size - Math.abs(index) : index;
            var finalSlotPtr = db.readLinkedArrayListSlot(header.ptr(), key, header.shift(), writeMode, isTopLevel);

            return db.readSlotPointer(writeMode, path, pathI + 1, finalSlotPtr.slotPtr());
        }
    }

    public static record LinkedArrayListAppend() implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            var nextArrayListStart = slotPtr.slot().value();

            // read header
            db.core.seek(nextArrayListStart);
            var headerBytes = new byte[LinkedArrayListHeader.length];
            reader.readFully(headerBytes);
            var origHeader = LinkedArrayListHeader.fromBytes(headerBytes);

            // append
            var appendResult = db.readLinkedArrayListSlotAppend(origHeader, writeMode, isTopLevel);
            var finalSlotPtr = db.readSlotPointer(writeMode, path, pathI + 1, appendResult.slotPtr().slotPtr());

            // update header
            var writer = db.core.writer();
            db.core.seek(nextArrayListStart);
            writer.write(appendResult.header().toBytes());

            return finalSlotPtr;
        }
    }

    public static record LinkedArrayListSlice(long offset, long size) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            var nextArrayListStart = slotPtr.slot().value();

            // read header
            db.core.seek(nextArrayListStart);
            var headerBytes = new byte[LinkedArrayListHeader.length];
            reader.readFully(headerBytes);
            var origHeader = LinkedArrayListHeader.fromBytes(headerBytes);

            // slice
            var sliceHeader = db.readLinkedArrayListSlice(origHeader, this.offset(), this.size());
            var finalSlotPtr = db.readSlotPointer(writeMode, path, pathI + 1, slotPtr);

            // update header
            var writer = db.core.writer();
            db.core.seek(nextArrayListStart);
            writer.write(sliceHeader.toBytes());

            return finalSlotPtr;
        }
    }

    public static record LinkedArrayListConcat(Slot list) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            if (this.list().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            var nextArrayListStart = slotPtr.slot().value();

            // read headers
            db.core.seek(nextArrayListStart);
            var headerBytesA = new byte[LinkedArrayListHeader.length];
            reader.readFully(headerBytesA);
            var headerA = LinkedArrayListHeader.fromBytes(headerBytesA);
            db.core.seek(this.list.value());
            var headerBytesB = new byte[LinkedArrayListHeader.length];
            reader.readFully(headerBytesB);
            var headerB = LinkedArrayListHeader.fromBytes(headerBytesB);

            // concat
            var concatHeader = db.readLinkedArrayListConcat(headerA, headerB);
            var finalSlotPtr = db.readSlotPointer(writeMode, path, pathI + 1, slotPtr);

            // update header
            var writer = db.core.writer();
            db.core.seek(nextArrayListStart);
            writer.write(concatHeader.toBytes());

            return finalSlotPtr;
        }
    }

    public static record LinkedArrayListInsert(long index) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            var nextArrayListStart = slotPtr.slot().value();

            // read header
            db.core.seek(nextArrayListStart);
            var headerBytes = new byte[LinkedArrayListHeader.length];
            reader.readFully(headerBytes);
            var origHeader = LinkedArrayListHeader.fromBytes(headerBytes);

            // get the key
            var index = this.index();
            if (index >= origHeader.size || index < -origHeader.size) {
                throw new KeyNotFoundException();
            }
            var key = index < 0 ? origHeader.size - Math.abs(index) : index;

            // split up the list
            var headerA = db.readLinkedArrayListSlice(origHeader, 0, key);
            var headerB = db.readLinkedArrayListSlice(origHeader, key, origHeader.size - key);

            // add new slot to first list
            var appendResult = db.readLinkedArrayListSlotAppend(headerA, writeMode, isTopLevel);

            // concat the lists
            var concatHeader = db.readLinkedArrayListConcat(appendResult.header(), headerB);

            // get pointer to the new slot
            var nextSlotPtr = db.readLinkedArrayListSlot(concatHeader.ptr(), key, concatHeader.shift(), WriteMode.READ_ONLY, isTopLevel);

            // recur down the rest of the path
            var finalSlotPtr = db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr.slotPtr());

            // update header
            var writer = db.core.writer();
            db.core.seek(nextArrayListStart);
            writer.write(concatHeader.toBytes());

            return finalSlotPtr;
        }
    }

    public static record LinkedArrayListRemove(long index) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            var nextArrayListStart = slotPtr.slot().value();

            // read header
            db.core.seek(nextArrayListStart);
            var headerBytes = new byte[LinkedArrayListHeader.length];
            reader.readFully(headerBytes);
            var origHeader = LinkedArrayListHeader.fromBytes(headerBytes);

            // get the key
            var index = this.index();
            if (index >= origHeader.size || index < -origHeader.size) {
                throw new KeyNotFoundException();
            }
            var key = index < 0 ? origHeader.size - Math.abs(index) : index;

            // split up the list
            var headerA = db.readLinkedArrayListSlice(origHeader, 0, key);
            var headerB = db.readLinkedArrayListSlice(origHeader, key + 1, origHeader.size - (key + 1));

            // concat the lists
            var concatHeader = db.readLinkedArrayListConcat(headerA, headerB);

            // get pointer to the new list
            var nextSlotPtr = new SlotPointer(concatHeader.ptr(), new Slot(nextArrayListStart, Tag.LINKED_ARRAY_LIST));

            // recur down the rest of the path
            var finalSlotPtr = db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);

            // update header
            var writer = db.core.writer();
            db.core.seek(nextArrayListStart);
            writer.write(concatHeader.toBytes());

            return finalSlotPtr;
        }
    }

    public static record HashMapInit(boolean counted, boolean set) implements PathPart {
        public HashMapInit() {
            this(false, false);
        }

        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            Tag tag = this.counted() ?
                        (this.set() ? Tag.COUNTED_HASH_SET : Tag.COUNTED_HASH_MAP) :
                        (this.set() ? Tag.HASH_SET : Tag.HASH_MAP);

            if (isTopLevel) {
                var writer = db.core.writer();

                // if the top level hash map hasn't been initialized
                if (db.header.tag == Tag.NONE) {
                    db.core.seek(DATABASE_START);

                    if (this.counted()) {
                        writer.writeLong(0);
                    }

                    // write the first block
                    writer.write(new byte[INDEX_BLOCK_SIZE]);

                    // update db header
                    db.core.seek(0);
                    db.header = db.header.withTag(tag);
                    writer.write(db.header.toBytes());
                }

                var nextSlotPtr = slotPtr.withSlot(slotPtr.slot().withTag(tag));
                return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
            }

            if (slotPtr.position() == null) throw new CursorNotWriteableException();
            long position = slotPtr.position();

            switch (slotPtr.slot().tag()) {
                case NONE -> {
                    // if slot was empty, insert the new map
                    var writer = db.core.writer();
                    var mapStart = db.core.length();
                    db.core.seek(mapStart);
                    if (this.counted()) {
                        writer.writeLong(0);
                    }
                    writer.write(new byte[INDEX_BLOCK_SIZE]);
                    // make slot point to map
                    var nextSlotPr = new SlotPointer(position, new Slot(mapStart, tag));
                    db.core.seek(position);
                    writer.write(nextSlotPr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPr);
                }
                case HASH_MAP, HASH_SET, COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                    if (this.counted()) {
                        switch (slotPtr.slot().tag()) {
                            case COUNTED_HASH_MAP, COUNTED_HASH_SET -> {}
                            default -> throw new UnexpectedTagException();
                        }
                    } else {
                        switch (slotPtr.slot().tag()) {
                            case HASH_MAP, HASH_SET -> {}
                            default -> throw new UnexpectedTagException();
                        }
                    }

                    var reader = db.core.reader();
                    var writer = db.core.writer();

                    var mapStart = slotPtr.slot().value();

                    // copy it to the end unless it was made in this transaction
                    if (db.txStart != null) {
                        if (mapStart < db.txStart) {
                            // read existing block
                            db.core.seek(mapStart);
                            Long mapCountMaybe = this.counted() ? reader.readLong() : null;
                            var mapIndexBlock = new byte[INDEX_BLOCK_SIZE];
                            reader.readFully(mapIndexBlock);
                            // copy to the end
                            mapStart = db.core.length();
                            db.core.seek(mapStart);
                            if (mapCountMaybe != null) writer.writeLong(mapCountMaybe);
                            writer.write(mapIndexBlock);
                        }
                    } else if (db.header.tag == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }

                    // make slot point to map
                    var nextSlotPtr = new SlotPointer(position, new Slot(mapStart, tag));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                default -> throw new UnexpectedTagException();
            }
        }
    }

    public static record HashMapGet(HashMapGetTarget target) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            boolean counted = false;
            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case HASH_MAP, HASH_SET -> {}
                case COUNTED_HASH_MAP, COUNTED_HASH_SET -> counted = true;
                default -> throw new UnexpectedTagException();
            }

            long indexPos = counted ? slotPtr.slot().value() + 8 : slotPtr.slot().value();

            var res = db.readMapSlot(indexPos, db.checkHash(this.target()), (byte)0, writeMode, isTopLevel, this.target());

            if (writeMode == WriteMode.READ_WRITE && counted && res.isEmpty()) {
                var reader = db.core.reader();
                var writer = db.core.writer();
                db.core.seek(slotPtr.slot().value());
                long mapCount = reader.readLong();
                db.core.seek(slotPtr.slot().value());
                writer.writeLong(mapCount + 1);
            }

            return db.readSlotPointer(writeMode, path, pathI + 1, res.slotPtr);
        }
    }

    public static record HashMapRemove(byte[] hash) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            boolean counted = false;
            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case HASH_MAP, HASH_SET -> {}
                case COUNTED_HASH_MAP, COUNTED_HASH_SET -> counted = true;
                default -> throw new UnexpectedTagException();
            }

            long indexPos = counted ? slotPtr.slot().value() + 8 : slotPtr.slot().value();

            boolean keyFound = true;
            try {
                db.removeMapSlot(indexPos, db.checkHash(this.hash()), (byte)0, isTopLevel);
            } catch (KeyNotFoundException e) {
                keyFound = false;
            }

            if (writeMode == WriteMode.READ_WRITE && counted && keyFound) {
                var reader = db.core.reader();
                var writer = db.core.writer();
                db.core.seek(slotPtr.slot().value());
                long mapCount = reader.readLong();
                db.core.seek(slotPtr.slot().value());
                writer.writeLong(mapCount - 1);
            }

            if (!keyFound) throw new KeyNotFoundException();

            return slotPtr;
        }
    }

    public static record WriteData(WriteableData data) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.position() == null) throw new CursorNotWriteableException();
            long position = slotPtr.position();

            var writer = db.core.writer();

            var data = this.data();
            Slot slot = null;
            if (data == null) {
                slot = new Slot();
            } else if (data instanceof Slot s) {
                slot = s;
            } else if (data instanceof Uint i) {
                if (i.value() < 0) {
                    throw new IllegalArgumentException("Uint must not be negative");
                }
                slot = new Slot(i.value(), Tag.UINT);
            } else if (data instanceof Int i) {
                slot = new Slot(i.value(), Tag.INT);
            } else if (data instanceof Float f) {
                var buffer = ByteBuffer.allocate(8);
                buffer.putDouble(f.value());
                buffer.position(0);
                slot = new Slot(buffer.getLong(), Tag.FLOAT);
            } else if (data instanceof Bytes bytes) {
                if (bytes.isShort()) {
                    var buffer = ByteBuffer.allocate(8);
                    buffer.put(bytes.value());
                    if (bytes.formatTag() != null) {
                        buffer.position(6);
                        buffer.put(bytes.formatTag());
                    }
                    buffer.position(0);
                    slot = new Slot(buffer.getLong(), Tag.SHORT_BYTES, bytes.formatTag() != null);
                } else {
                    var nextCursor = new WriteCursor(slotPtr, db);
                    var cursorWriter = nextCursor.writer();
                    cursorWriter.formatTag = bytes.formatTag(); // the writer will write the format tag when finish is called
                    cursorWriter.write(bytes.value());
                    cursorWriter.finish();
                    slot = cursorWriter.slot;
                }
            } else {
                throw new IllegalArgumentException();
            }

            // this bit allows us to distinguish between a slot explicitly set to NONE
            // and a slot that hasn't been set yet
            if (slot.tag() == Tag.NONE) {
                slot = slot.withFull(true);
            }

            db.core.seek(position);
            writer.write(slot.toBytes());

            var nextSlotPtr = new SlotPointer(slotPtr.position(), slot);
            return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
        }
    }

    public static record Context(ContextFunction function) implements PathPart {
        public SlotPointer readSlotPointer(Database db, boolean isTopLevel, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (pathI != path.length - 1) throw new PathPartMustBeAtEndException();

            var nextCursor = new WriteCursor(slotPtr, db);
            try {
                this.function().run(nextCursor);
            } catch (Exception e) {
                // since an error occured, there may be inaccessible
                // junk at the end of the db, so delete it if possible
                try {
                    db.truncate();
                } catch (Exception e2) {}
                throw e;
            }
            return nextCursor.slotPtr;
        }
    }

    public static interface ContextFunction {
        public void run(WriteCursor cursor) throws Exception;
    }

    public static sealed interface HashMapGetTarget permits HashMapGetKVPair, HashMapGetKey, HashMapGetValue {}
    public static record HashMapGetKVPair(byte[] hash) implements HashMapGetTarget {}
    public static record HashMapGetKey(byte[] hash) implements HashMapGetTarget {}
    public static record HashMapGetValue(byte[] hash) implements HashMapGetTarget {}

    public static sealed interface WriteableData permits Slot, Uint, Int, Float, Bytes {}
    public static record Uint(long value) implements WriteableData {}
    public static record Int(long value) implements WriteableData {}
    public static record Float(double value) implements WriteableData {}
    public static record Bytes(byte[] value, byte[] formatTag) implements WriteableData {
        public Bytes(String value) throws UnsupportedEncodingException {
            this(value, null);
        }

        public Bytes(String value, String formatTag) throws UnsupportedEncodingException {
            this(value.getBytes("UTF-8"), formatTag == null ? null : formatTag.getBytes("UTF-8"));
        }

        public Bytes(byte[] value) {
            this(value, null);
        }

        public Bytes(byte[] value, byte[] formatTag) {
            if (formatTag != null && formatTag.length != 2) throw new InvalidFormatTagSizeException();
            this.value = value;
            this.formatTag = formatTag;
        }

        public boolean isShort() {
            var totalSize = this.formatTag != null ? 6 : 8;
            if (this.value.length > totalSize) return false;
            for (byte b : this.value) {
                if (b == 0) return false;
            }
            return true;
        }
    }

    public static record LinkedArrayListSlot(long size, Slot slot) {
        public static int length = 8 + Slot.length;

        public LinkedArrayListSlot withSize(long size) {
            return new LinkedArrayListSlot(size, this.slot);
        }

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.slot.toBytes());
            buffer.putLong(this.size);
            return buffer.array();
        }

        public static LinkedArrayListSlot fromBytes(byte[] bytes) {
            var buffer = ByteBuffer.wrap(bytes);
            var slotBytes = new byte[Slot.length];
            buffer.get(slotBytes);
            var slot = Slot.fromBytes(slotBytes);
            var size = checkLong(buffer.getLong());
            return new LinkedArrayListSlot(size, slot);
        }
    }

    public static record LinkedArrayListSlotPointer(SlotPointer slotPtr, long leafCount) {
        public LinkedArrayListSlotPointer withSlotPointer(SlotPointer slotPtr) {
            return new LinkedArrayListSlotPointer(slotPtr, this.leafCount);
        }
    }

    public static record LinkedArrayListBlockInfo(LinkedArrayListSlot[] block, byte i, LinkedArrayListSlot parentSlot) {}

    // exceptions

    public static class DatabaseException extends RuntimeException {}
    public static class NotImplementedException extends DatabaseException {}
    public static class UnreachableException extends DatabaseException {}
    public static class InvalidDatabaseException extends DatabaseException {}
    public static class InvalidVersionException extends DatabaseException {}
    public static class InvalidHashSizeException extends DatabaseException {}
    public static class KeyNotFoundException extends DatabaseException {}
    public static class WriteNotAllowedException extends DatabaseException {}
    public static class UnexpectedTagException extends DatabaseException {}
    public static class CursorNotWriteableException extends DatabaseException {}
    public static class ExpectedTxStartException extends DatabaseException {}
    public static class KeyOffsetExceededException extends DatabaseException {}
    public static class PathPartMustBeAtEndException extends DatabaseException {}
    public static class StreamTooLongException extends DatabaseException {}
    public static class EndOfStreamException extends DatabaseException {}
    public static class InvalidOffsetException extends DatabaseException {}
    public static class InvalidTopLevelTypeException extends DatabaseException {}
    public static class ExpectedUnsignedLongException extends DatabaseException {}
    public static class NoAvailableSlotsException extends DatabaseException {}
    public static class MustSetNewSlotsToFullException extends DatabaseException {}
    public static class EmptySlotException extends DatabaseException {}
    public static class ExpectedRootNodeException extends DatabaseException {}
    public static class InvalidFormatTagSizeException extends DatabaseException {}
    public static class UnexpectedWriterPositionException extends DatabaseException {}
    public static class MaxShiftExceededException extends DatabaseException {}

    // hash_map

    public static record HashMapGetResult(SlotPointer slotPtr, boolean isEmpty) {}

    private HashMapGetResult readMapSlot(long indexPos, byte[] keyHash, byte keyOffset, WriteMode writeMode, boolean isTopLevel, HashMapGetTarget target) throws IOException {
        if (keyOffset > (this.header.hashSize() * 8) / BIT_COUNT) {
            throw new KeyOffsetExceededException();
        }

        var reader = this.core.reader();
        var writer = this.core.writer();

        var i = new BigInteger(keyHash).shiftRight(keyOffset * BIT_COUNT).and(BIG_MASK).intValueExact();
        var slotPos = indexPos + (Slot.length * i);
        this.core.seek(slotPos);
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        var slot = Slot.fromBytes(slotBytes);

        var ptr = slot.value();

        switch (slot.tag()) {
            case NONE -> {
                switch (writeMode) {
                    case READ_ONLY -> throw new KeyNotFoundException();
                    case READ_WRITE -> {
                        // write hash and key/val slots
                        var hashPos = this.core.length();
                        this.core.seek(hashPos);
                        var keySlotPos = hashPos + this.header.hashSize();
                        var valueSlotPos = keySlotPos + Slot.length;
                        var kvPair = new KeyValuePair(new Slot(), new Slot(), keyHash);
                        writer.write(kvPair.toBytes());

                        // point slot to hash pos
                        var nextSlot = new Slot(hashPos, Tag.KV_PAIR);
                        this.core.seek(slotPos);
                        writer.write(nextSlot.toBytes());

                        SlotPointer nextSlotPtr = null;
                        if (target instanceof HashMapGetKVPair) {
                            nextSlotPtr = new SlotPointer(slotPos, nextSlot);
                        } else if (target instanceof HashMapGetKey) {
                            nextSlotPtr = new SlotPointer(keySlotPos, kvPair.keySlot());
                        } else if (target instanceof HashMapGetValue) {
                            nextSlotPtr = new SlotPointer(valueSlotPos, kvPair.valueSlot());
                        } else {
                            throw new IllegalArgumentException();
                        }
                        return new HashMapGetResult(nextSlotPtr, true);
                    }
                    default -> throw new UnreachableException();
                }
            }
            case INDEX -> {
                var nextPtr = ptr;
                if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                    if (this.txStart != null) {
                        if (nextPtr < this.txStart) {
                            // read existing block
                            this.core.seek(ptr);
                            var indexBlock = new byte[INDEX_BLOCK_SIZE];
                            reader.readFully(indexBlock);
                            // copy it to the end
                            nextPtr = this.core.length();
                            this.core.seek(nextPtr);
                            writer.write(indexBlock);
                            // make slot point to block
                            this.core.seek(slotPos);
                            writer.write(new Slot(nextPtr, Tag.INDEX).toBytes());
                        }
                    } else if (this.header.tag() == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }
                }
                return readMapSlot(nextPtr, keyHash, (byte) (keyOffset + 1), writeMode, isTopLevel, target);
            }
            case KV_PAIR -> {
                this.core.seek(ptr);
                var kvPairBytes = new byte[KeyValuePair.length(this.header.hashSize())];
                reader.readFully(kvPairBytes);
                var kvPair = KeyValuePair.fromBytes(kvPairBytes, this.header.hashSize());

                if (Arrays.equals(kvPair.hash(), keyHash)) {
                    if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                        if (this.txStart != null) {
                            if (ptr < this.txStart) {
                                // write hash and key/val slots
                                var hashPos = this.core.length();
                                this.core.seek(hashPos);
                                var keySlotPos = hashPos + this.header.hashSize();
                                var valueSlotPos = keySlotPos + Slot.length;
                                writer.write(kvPair.toBytes());

                                // point slot to hash pos
                                var nextSlot = new Slot(hashPos, Tag.KV_PAIR);
                                this.core.seek(slotPos);
                                writer.write(nextSlot.toBytes());

                                SlotPointer nextSlotPtr = null;
                                if (target instanceof HashMapGetKVPair) {
                                    nextSlotPtr = new SlotPointer(slotPos, nextSlot);
                                } else if (target instanceof HashMapGetKey) {
                                    nextSlotPtr = new SlotPointer(keySlotPos, kvPair.keySlot());
                                } else if (target instanceof HashMapGetValue) {
                                    nextSlotPtr = new SlotPointer(valueSlotPos, kvPair.valueSlot());
                                } else {
                                    throw new IllegalArgumentException();
                                }
                                return new HashMapGetResult(nextSlotPtr, false);
                            }
                        } else if (this.header.tag() == Tag.ARRAY_LIST) {
                            throw new ExpectedTxStartException();
                        }
                    }

                    var keySlotPos = ptr + this.header.hashSize();
                    var valueSlotPos = keySlotPos + Slot.length;
                    SlotPointer nextSlotPtr = null;
                    if (target instanceof HashMapGetKVPair) {
                        nextSlotPtr = new SlotPointer(slotPos, slot);
                    } else if (target instanceof HashMapGetKey) {
                        nextSlotPtr = new SlotPointer(keySlotPos, kvPair.keySlot());
                    } else if (target instanceof HashMapGetValue) {
                        nextSlotPtr = new SlotPointer(valueSlotPos, kvPair.valueSlot());
                    } else {
                        throw new IllegalArgumentException();
                    }
                    return new HashMapGetResult(nextSlotPtr, false);
                } else {
                    switch (writeMode) {
                        case READ_ONLY -> throw new KeyNotFoundException();
                        case READ_WRITE -> {
                            // append new index block
                            if (keyOffset + 1 >= (this.header.hashSize() * 8) / BIT_COUNT) {
                                throw new KeyOffsetExceededException();
                            }
                            var nextI = new BigInteger(kvPair.hash()).shiftRight((keyOffset + 1) * BIT_COUNT).and(BIG_MASK).intValueExact();
                            var nextIndexPos = this.core.length();
                            this.core.seek(nextIndexPos);
                            writer.write(new byte[INDEX_BLOCK_SIZE]);
                            this.core.seek(nextIndexPos + (Slot.length * nextI));
                            writer.write(slot.toBytes());
                            var res = readMapSlot(nextIndexPos, keyHash, (byte) (keyOffset + 1), writeMode, isTopLevel, target);
                            this.core.seek(slotPos);
                            writer.write(new Slot(nextIndexPos, Tag.INDEX).toBytes());
                            return res;
                        }
                        default -> throw new UnreachableException();
                    }
                }
            }
            default -> throw new UnexpectedTagException();
        }
    }

    private Slot removeMapSlot(long indexPos, byte[] keyHash, byte keyOffset, boolean isTopLevel) throws IOException {
        if (keyOffset > (this.header.hashSize() * 8) / BIT_COUNT) {
            throw new KeyOffsetExceededException();
        }

        var reader = this.core.reader();
        var writer = this.core.writer();

        // read block
        var slotBlock = new Slot[SLOT_COUNT];
        this.core.seek(indexPos);
        var indexBlock = new byte[INDEX_BLOCK_SIZE];
        reader.readFully(indexBlock);
        var buffer = ByteBuffer.wrap(indexBlock);
        for (int i = 0; i < slotBlock.length; i++) {
            var slotBytes = new byte[Slot.length];
            buffer.get(slotBytes);
            slotBlock[i] = Slot.fromBytes(slotBytes);
        }

        // get the current slot
        var i = new BigInteger(keyHash).shiftRight(keyOffset * BIT_COUNT).and(BIG_MASK).intValueExact();
        var slotPos = indexPos + (Slot.length * i);
        var slot = slotBlock[i];

        // get the slot that will replace the current slot
        var nextSlot = switch (slot.tag()) {
            case NONE -> throw new KeyNotFoundException();
            case INDEX -> removeMapSlot(slot.value(), keyHash, (byte) (keyOffset + 1), isTopLevel);
            case KV_PAIR -> {
                this.core.seek(slot.value());
                var kvPairBytes = new byte[KeyValuePair.length(this.header.hashSize())];
                reader.readFully(kvPairBytes);
                var kvPair = KeyValuePair.fromBytes(kvPairBytes, this.header.hashSize());
                if (Arrays.equals(kvPair.hash(), keyHash)) {
                    yield new Slot();
                } else {
                    throw new KeyNotFoundException();
                }
            }
            default -> throw new UnexpectedTagException();
        };

        // if we're the root node, just write the new slot and finish
        if (keyOffset == 0) {
            this.core.seek(slotPos);
            writer.write(nextSlot.toBytes());
            return new Slot(indexPos, Tag.INDEX);
        }

        // get slot to return if there is only one used slot
        // in this index block
        var slotToReturnMaybe = new Slot();
        slotBlock[i] = nextSlot;
        for (Slot blockSlot : slotBlock) {
            if (blockSlot.tag() == Tag.NONE) continue;

            // if there is already a slot to return, that
            // means there is more than one used slot in this
            // index block, so we can't return just a single slot
            if (slotToReturnMaybe != null) {
                if (slotToReturnMaybe.tag() != Tag.NONE) {
                    slotToReturnMaybe = null;
                    break;
                }
            }

            slotToReturnMaybe = blockSlot;
        }

        // if there were either no used slots, or a single KV_PAIR
        // slot, this index block doesn't need to exist anymore
        if (slotToReturnMaybe != null) {
            switch (slotToReturnMaybe.tag()) {
                case NONE, KV_PAIR -> {
                    return slotToReturnMaybe;
                }
                default -> {}
            }
        }

        // there was more than one used slot, or a single INDEX slot,
        // so we must keep this index block

        if (!isTopLevel) {
            if (this.txStart != null) {
                if (indexPos < this.txStart) {
                    // copy index block to the end
                    var nextIndexPos = this.core.length();
                    this.core.seek(nextIndexPos);
                    writer.write(indexBlock);
                    // update the slot
                    var nextSlotPos = nextIndexPos + (Slot.length * i);
                    this.core.seek(nextSlotPos);
                    writer.write(nextSlot.toBytes());
                    return new Slot(nextIndexPos, Tag.INDEX);
                }
            } else if (this.header.tag() == Tag.ARRAY_LIST) {
                throw new ExpectedTxStartException();
            }
        }

        this.core.seek(slotPos);
        writer.write(nextSlot.toBytes());
        return new Slot(indexPos, Tag.INDEX);
    }

    // array_list

    public static record ArrayListAppendResult(ArrayListHeader header, SlotPointer slotPtr) {}

    private ArrayListAppendResult readArrayListSlotAppend(ArrayListHeader header, WriteMode writeMode, boolean isTopLevel) throws IOException {
        var writer = this.core.writer();

        var indexPos = header.ptr();

        var key = header.size;

        var prevShift = (byte) (key < SLOT_COUNT ? 0 : Math.log(key - 1) / Math.log(SLOT_COUNT));
        var nextShift = (byte) (key < SLOT_COUNT ? 0 : Math.log(key) / Math.log(SLOT_COUNT));

        if (prevShift != nextShift) {
            // root overflow
            var nextIndexPos = this.core.length();
            this.core.seek(nextIndexPos);
            writer.write(new byte[INDEX_BLOCK_SIZE]);
            this.core.seek(nextIndexPos);
            writer.write(new Slot(indexPos, Tag.INDEX).toBytes());
            indexPos = nextIndexPos;
        }

        var slotPtr = readArrayListSlot(indexPos, key, nextShift, writeMode, isTopLevel);
        return new ArrayListAppendResult(new ArrayListHeader(indexPos, header.size() + 1), slotPtr);
    }

    private SlotPointer readArrayListSlot(long indexPos, long key, byte shift, WriteMode writeMode, boolean isTopLevel) throws IOException {
        if (shift >= MAX_BRANCH_LENGTH) throw new MaxShiftExceededException();

        var reader = this.core.reader();

        var i = (key >> (shift * BIT_COUNT)) & MASK;
        var slotPos = indexPos + (Slot.length * i);
        this.core.seek(slotPos);
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        var slot = Slot.fromBytes(slotBytes);

        if (shift == 0) {
            return new SlotPointer(slotPos, slot);
        }

        var ptr = slot.value();

        switch (slot.tag()) {
            case NONE -> {
                switch (writeMode) {
                    case READ_ONLY -> throw new KeyNotFoundException();
                    case READ_WRITE -> {
                        var writer = this.core.writer();
                        var nextIndexPos = this.core.length();
                        this.core.seek(nextIndexPos);
                        writer.write(new byte[INDEX_BLOCK_SIZE]);
                        // if top level array list, update the file size in the list
                        // header to prevent truncation from destroying this block
                        if (isTopLevel) {
                            var fileSize = this.core.length();
                            this.core.seek(DATABASE_START + ArrayListHeader.length);
                            writer.writeLong(fileSize);
                        }
                        this.core.seek(slotPos);
                        writer.write(new Slot(nextIndexPos, Tag.INDEX).toBytes());
                        return readArrayListSlot(nextIndexPos, key, (byte)(shift - 1), writeMode, isTopLevel);
                    }
                    default -> throw new UnreachableException();
                }
            }
            case INDEX -> {
                var nextPtr = ptr;
                if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                    if (this.txStart != null) {
                        if (nextPtr < this.txStart) {
                            // read existing block
                            this.core.seek(ptr);
                            var indexBlock = new byte[INDEX_BLOCK_SIZE];
                            reader.readFully(indexBlock);
                            // copy it to the end
                            var writer = this.core.writer();
                            nextPtr = this.core.length();
                            this.core.seek(nextPtr);
                            writer.write(indexBlock);
                            // make slot point to block
                            this.core.seek(slotPos);
                            writer.write(new Slot(nextPtr, Tag.INDEX).toBytes());
                        }
                    } else if (this.header.tag() == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }
                }
                return readArrayListSlot(nextPtr, key, (byte)(shift - 1), writeMode, isTopLevel);
            }
            default -> throw new UnexpectedTagException();
        }
    }

    private ArrayListHeader readArrayListSlice(ArrayListHeader header, long size) throws IOException {
        var reader = this.core.reader();

        if (size > header.size() || size < 0) {
            throw new KeyNotFoundException();
        }

        var prevShift = (byte) (header.size < SLOT_COUNT + 1 ? 0 : Math.log(header.size - 1) / Math.log(SLOT_COUNT));
        var nextShift = (byte) (size < SLOT_COUNT + 1 ? 0 : Math.log(size - 1) / Math.log(SLOT_COUNT));

        if (prevShift == nextShift) {
            // the root node doesn't need to change
            return new ArrayListHeader(header.ptr, size);
        } else {
            // keep following the first slot until we are at the correct shift
            var shift = prevShift;
            var indexPos = header.ptr;
            while (shift > nextShift) {
                this.core.seek(indexPos);
                var slotBytes = new byte[Slot.length];
                reader.readFully(slotBytes);
                var slot = Slot.fromBytes(slotBytes);
                shift -= 1;
                indexPos = slot.value();
            }
            return new ArrayListHeader(indexPos, size);
        }
    }

    // linked_array_list

    public static record LinkedArrayListAppendResult(LinkedArrayListHeader header, LinkedArrayListSlotPointer slotPtr) {}

    private LinkedArrayListAppendResult readLinkedArrayListSlotAppend(LinkedArrayListHeader header, WriteMode writeMode, boolean isTopLevel) throws IOException {
        var writer = this.core.writer();

        var ptr = header.ptr;
        var key = header.size;
        var shift = header.shift;

        LinkedArrayListSlotPointer slotPtr = null;
        try {
            slotPtr = readLinkedArrayListSlot(ptr, key, shift, writeMode, isTopLevel);
        } catch (NoAvailableSlotsException e) {
            // root overflow
            var nextPtr = this.core.length();
            this.core.seek(nextPtr);
            writer.write(new byte[LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE]);
            this.core.seek(nextPtr);
            writer.write(new LinkedArrayListSlot(header.size, new Slot(ptr, Tag.INDEX, true)).toBytes());
            ptr = nextPtr;
            shift += 1;
            slotPtr = readLinkedArrayListSlot(ptr, key, shift, writeMode, isTopLevel);
        }

        // newly-appended slots must have full set to true
        // or else indexing will be screwed up
        var newSlot = new Slot(0, Tag.NONE, true);
        slotPtr = slotPtr.withSlotPointer(slotPtr.slotPtr().withSlot(newSlot));
        if (slotPtr.slotPtr().position() == null) throw new CursorNotWriteableException();
        long position = slotPtr.slotPtr().position();
        this.core.seek(position);
        writer.write(new LinkedArrayListSlot(0, newSlot).toBytes());
        if (header.size < SLOT_COUNT && shift > 0) {
            throw new MustSetNewSlotsToFullException();
        }

        return new LinkedArrayListAppendResult(
            new LinkedArrayListHeader(shift, ptr, header.size + 1),
            slotPtr
        );
    }

    private static long blockLeafCount(LinkedArrayListSlot[] block, byte shift, byte i) {
        long n = 0;
        // for leaf nodes, count all non-empty slots along with the slot being accessed
        if (shift == 0) {
            for (int blockI = 0; blockI < block.length; blockI++) {
                var blockSlot = block[blockI];
                if (!blockSlot.slot().empty() || blockI == i) {
                    n += 1;
                }
            }
        }
        // for non-leaf nodes, add up their sizes
        else {
            for (LinkedArrayListSlot blockSlot : block) {
                n += blockSlot.size();
            }
        }
        return n;
    }

    private static long slotLeafCount(LinkedArrayListSlot slot, byte shift) {
        if (shift == 0) {
            if (slot.slot().empty()) {
                return 0;
            } else {
                return 1;
            }
        } else {
            return slot.size();
        }
    }

    private static record KeyAndIndex(long key, byte index) {}

    private static KeyAndIndex keyAndIndexForLinkedArrayList(LinkedArrayListSlot[] slotBlock, long key, byte shift) {
        long nextKey = key;
        byte i = 0;
        long maxLeafCount = (long) (shift == 0 ? 1 : Math.pow(SLOT_COUNT, shift));
        while (true) {
            var slotLeafCount = slotLeafCount(slotBlock[i], shift);
            if (nextKey == slotLeafCount) {
                // if the slot's leaf count is at its maximum
                // or it is full, we have to skip to the next slot
                if (slotLeafCount == maxLeafCount || slotBlock[i].slot().full()) {
                    if (i < SLOT_COUNT - 1) {
                        nextKey -= slotLeafCount;
                        i += 1;
                    } else {
                        return null;
                    }
                }
                break;
            } else if (nextKey < slotLeafCount) {
                break;
            } else if (i < SLOT_COUNT - 1) {
                nextKey -= slotLeafCount;
                i += 1;
            } else {
                return null;
            }
        }
        return new KeyAndIndex(nextKey, i);
    }

    private LinkedArrayListSlotPointer readLinkedArrayListSlot(long indexPos, long key, byte shift, WriteMode writeMode, boolean isTopLevel) throws IOException {
        if (shift >= MAX_BRANCH_LENGTH) throw new MaxShiftExceededException();

        var reader = this.core.reader();
        var writer = this.core.writer();

        var slotBlock = new LinkedArrayListSlot[SLOT_COUNT];
        {
            this.core.seek(indexPos);
            var indexBlock = new byte[LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE];
            reader.readFully(indexBlock);

            var buffer = ByteBuffer.wrap(indexBlock);
            for (int i = 0; i < slotBlock.length; i++) {
                var slotBytes = new byte[LinkedArrayListSlot.length];
                buffer.get(slotBytes);
                slotBlock[i] = LinkedArrayListSlot.fromBytes(slotBytes);
            }
        }

        var keyAndIndex = keyAndIndexForLinkedArrayList(slotBlock, key, shift);
        if (keyAndIndex == null) throw new NoAvailableSlotsException();
        var nextKey = keyAndIndex.key;
        var i = keyAndIndex.index;
        var slot = slotBlock[i];
        var slotPos = indexPos + (LinkedArrayListSlot.length * i);

        if (shift == 0) {
            var leafCount = blockLeafCount(slotBlock, shift, i);
            return new LinkedArrayListSlotPointer(new SlotPointer(slotPos, slot.slot()), leafCount);
        }

        var ptr = slot.slot().value();

        switch (slot.slot().tag()) {
            case NONE -> {
                switch (writeMode) {
                    case READ_ONLY -> throw new KeyNotFoundException();
                    case READ_WRITE -> {
                        var nextIndexPos = this.core.length();
                        this.core.seek(nextIndexPos);
                        writer.write(new byte[LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE]);

                        var nextSlotPtr = readLinkedArrayListSlot(nextIndexPos, nextKey, (byte) (shift - 1), writeMode, isTopLevel);
                        slotBlock[i] = slotBlock[i].withSize(nextSlotPtr.leafCount());
                        var leafCount = blockLeafCount(slotBlock, shift, i);
                        this.core.seek(slotPos);
                        writer.write(new LinkedArrayListSlot(nextSlotPtr.leafCount(), new Slot(nextIndexPos, Tag.INDEX)).toBytes());
                        return new LinkedArrayListSlotPointer(nextSlotPtr.slotPtr(), leafCount);
                    }
                    default -> throw new UnreachableException();
                }
            }
            case INDEX -> {
                var nextPtr = ptr;
                if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                    if (this.txStart != null) {
                        if (nextPtr < this.txStart) {
                            // read existing block
                            this.core.seek(ptr);
                            var indexBlock = new byte[LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE];
                            reader.readFully(indexBlock);
                            // copy it to the end
                            nextPtr = this.core.length();
                            this.core.seek(nextPtr);
                            writer.write(indexBlock);
                        }
                    } else if (this.header.tag() == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }
                }

                var nextSlotPtr = readLinkedArrayListSlot(nextPtr, nextKey, (byte) (shift - 1), writeMode, isTopLevel);

                slotBlock[i] = slotBlock[i].withSize(nextSlotPtr.leafCount());
                var leafCount = blockLeafCount(slotBlock, shift, i);

                if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                    // make slot point to block
                    this.core.seek(slotPos);
                    writer.write(new LinkedArrayListSlot(nextSlotPtr.leafCount(), new Slot(nextPtr, Tag.INDEX)).toBytes());
                }

                return new LinkedArrayListSlotPointer(nextSlotPtr.slotPtr(), leafCount);
            }
            default -> throw new UnexpectedTagException();
        }
    }

    private void readLinkedArrayListBlocks(long indexPos, long key, byte shift, ArrayList<LinkedArrayListBlockInfo> blocks) throws IOException {
        var reader = this.core.reader();

        var slotBlock = new LinkedArrayListSlot[SLOT_COUNT];
        {
            this.core.seek(indexPos);
            var indexBlock = new byte[LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE];
            reader.readFully(indexBlock);

            var buffer = ByteBuffer.wrap(indexBlock);
            for (int i = 0; i < slotBlock.length; i++) {
                var slotBytes = new byte[LinkedArrayListSlot.length];
                buffer.get(slotBytes);
                slotBlock[i] = LinkedArrayListSlot.fromBytes(slotBytes);
            }
        }

        var keyAndIndex = keyAndIndexForLinkedArrayList(slotBlock, key, shift);
        if (keyAndIndex == null) throw new NoAvailableSlotsException();
        var nextKey = keyAndIndex.key;
        var i = keyAndIndex.index;
        var leafCount = blockLeafCount(slotBlock, shift, i);

        blocks.add(new LinkedArrayListBlockInfo(slotBlock, i, new LinkedArrayListSlot(leafCount, new Slot(indexPos, Tag.INDEX))));

        if (shift == 0) {
            return;
        }

        var slot = slotBlock[i];
        switch (slot.slot().tag()) {
            case NONE -> throw new EmptySlotException();
            case INDEX -> readLinkedArrayListBlocks(slot.slot().value(), nextKey, (byte) (shift - 1), blocks);
            default -> throw new UnexpectedTagException();
        }
    }

    private void populateArray(LinkedArrayListSlot[] arr) {
        for (int i = 0; i < arr.length; i++) {
            arr[i] = new LinkedArrayListSlot(0, new Slot());
        }
    }

    private LinkedArrayListHeader readLinkedArrayListSlice(LinkedArrayListHeader header, long offset, long size) throws IOException {
        var writer = this.core.writer();

        if (offset + size > header.size) {
            throw new KeyNotFoundException();
        }

        // read the list's left blocks
        var leftBlocks = new ArrayList<LinkedArrayListBlockInfo>();
        readLinkedArrayListBlocks(header.ptr, offset, header.shift, leftBlocks);

        // read the list's right blocks
        var rightBlocks = new ArrayList<LinkedArrayListBlockInfo>();
        var rightKey = offset + size == 0 ? 0 : offset + size - 1;
        readLinkedArrayListBlocks(header.ptr, rightKey, header.shift, rightBlocks);

        // create the new blocks
        var blockCount = leftBlocks.size();
        var nextSlots = new LinkedArrayListSlot[]{ null, null };
        byte nextShift = 0;
        for (int i = 0; i < blockCount; i++) {
            var isLeafNode = nextSlots[0] == null;

            var leftBlock = leftBlocks.get(blockCount - i - 1);
            var rightBlock = rightBlocks.get(blockCount - i - 1);
            var origBlockInfos = new LinkedArrayListBlockInfo[]{
                leftBlock,
                rightBlock
            };
            var nextBlocks = new LinkedArrayListSlot[][]{ null, null };

            if (leftBlock.parentSlot().slot().value() == rightBlock.parentSlot().slot().value()) {
                int slotI = 0;
                var newRootBlock = new LinkedArrayListSlot[SLOT_COUNT];
                populateArray(newRootBlock);
                // left slot
                if (size > 0) {
                    if (nextSlots[0] != null) {
                        newRootBlock[slotI] = nextSlots[0];
                    } else {
                        newRootBlock[slotI] = leftBlock.block()[leftBlock.i()];
                    }
                    slotI += 1;
                }
                if (size > 1) {
                    // middle slots
                    for (int j = leftBlock.i() + 1; j < rightBlock.i(); j++) {
                        var middleSlot = leftBlock.block()[j];
                        newRootBlock[slotI] = middleSlot;
                        slotI += 1;
                    }

                    // right slot
                    if (nextSlots[1] != null) {
                        newRootBlock[slotI] = nextSlots[1];
                    } else {
                        newRootBlock[slotI] = leftBlock.block()[rightBlock.i()];
                    }
                }
                nextBlocks[0] = newRootBlock;
            } else {
                int slotI = 0;
                var newLeftBlock = new LinkedArrayListSlot[SLOT_COUNT];
                populateArray(newLeftBlock);

                // first slot
                if (nextSlots[0] != null) {
                    newLeftBlock[slotI] = nextSlots[0];
                } else {
                    newLeftBlock[slotI] = leftBlock.block()[leftBlock.i()];
                }
                slotI += 1;
                // rest of slots
                for (var j = leftBlock.i() + 1; j < leftBlock.block().length; j++) {
                    var nextSlot = leftBlock.block()[j];
                    newLeftBlock[slotI] = nextSlot;
                    slotI += 1;
                }
                nextBlocks[0] = newLeftBlock;

                slotI = 0;
                var newRightBlock = new LinkedArrayListSlot[SLOT_COUNT];
                populateArray(newRightBlock);
                // first slots
                for (var j = 0; j < rightBlock.i(); j++) {
                    var firstSlot = rightBlock.block()[j];
                    newRightBlock[slotI] = firstSlot;
                    slotI += 1;
                }
                // last slot
                if (nextSlots[1] != null) {
                    newRightBlock[slotI] = nextSlots[1];
                } else {
                    newRightBlock[slotI] = rightBlock.block()[rightBlock.i()];
                }
                nextBlocks[1] = newRightBlock;

                nextShift += 1;
            }

            // clear the next slots
            nextSlots = new LinkedArrayListSlot[]{ null, null };

            // write the block(s)
            this.core.seek(this.core.length());
            for (int j = 0; j < 2; j++) {
                var blockMaybe = nextBlocks[j];
                var origBlockInfo = origBlockInfos[j];

                if (blockMaybe != null) {
                    // determine if the block changed compared to the original block
                    boolean eql = true;
                    for (int k = 0; k < blockMaybe.length; k++) {
                        var blockSlot = blockMaybe[k];
                        var origSlot = origBlockInfo.block()[k];
                        if (!blockSlot.slot().equals(origSlot.slot())) {
                            eql = false;
                            break;
                        }
                    }
                    // if there is no change, just use the original block
                    if (eql) {
                        nextSlots[j] = origBlockInfo.parentSlot();
                    }
                    // otherwise make a new block
                    else {
                        var nextPtr = this.core.position();
                        long leafCount = 0;
                        for (int k = 0; k < blockMaybe.length; k++) {
                            var blockSlot = blockMaybe[k];
                            writer.write(blockSlot.toBytes());
                            if (isLeafNode) {
                                if (!blockSlot.slot().empty()) {
                                    leafCount += 1;
                                }
                            } else {
                                leafCount += blockSlot.size();
                            }
                        }
                        nextSlots[j] = new LinkedArrayListSlot(
                            leafCount,
                            // only the left side needs to be full,
                            // because it can have a gap that affects indexing
                            switch (j) {
                                // left
                                case 0 -> new Slot(nextPtr, Tag.INDEX, true);
                                // right
                                case 1 -> new Slot(nextPtr, Tag.INDEX);
                                default -> throw new UnreachableException();
                            }
                        );
                    }
                }
            }

            // we found the root node so we can exit
            if (nextSlots[0] != null && nextSlots[1] == null) {
                break;
            }
        }

        var rootSlot = nextSlots[0];
        if (rootSlot == null) throw new ExpectedRootNodeException();

        return new LinkedArrayListHeader(nextShift, rootSlot.slot().value(), size);
    }

    private LinkedArrayListHeader readLinkedArrayListConcat(LinkedArrayListHeader headerA, LinkedArrayListHeader headerB) throws IOException {
        var writer = this.core.writer();

        // read the first list's blocks
        var blocksA = new ArrayList<LinkedArrayListBlockInfo>();
        var keyA = headerA.size() == 0 ? 0 : headerA.size() - 1;
        readLinkedArrayListBlocks(headerA.ptr(), keyA, headerA.shift(), blocksA);

        // read the second list's blocks
        var blocksB = new ArrayList<LinkedArrayListBlockInfo>();
        readLinkedArrayListBlocks(headerB.ptr(), 0, headerB.shift(), blocksB);

        // stitch the blocks together
        var nextSlots = new LinkedArrayListSlot[]{ null, null };
        byte nextShift = 0;
        for (int i = 0; i < Math.max(blocksA.size(), blocksB.size()); i++) {
            var blockInfos = new LinkedArrayListBlockInfo[]{
                i < blocksA.size() ? blocksA.get(blocksA.size() - 1 - i) : null,
                i < blocksB.size() ? blocksB.get(blocksB.size() - 1 - i) : null
            };
            var nextBlocks = new LinkedArrayListSlot[][]{ null, null };
            var isLeafNode = nextSlots[0] == null;

            if (!isLeafNode) {
                nextShift += 1;
            }

            for (int j = 0; j < 2; j++) {
                var blockInfoMaybe = blockInfos[j];
                if (blockInfoMaybe != null) {
                    var block = new LinkedArrayListSlot[SLOT_COUNT];
                    populateArray(block);
                    int targetI = 0;
                    for (int sourceI = 0; sourceI < blockInfoMaybe.block().length; sourceI++) {
                        var blockSlot = blockInfoMaybe.block()[sourceI];
                        // skip the i'th block if necessary
                        if (!isLeafNode && blockInfoMaybe.i() == sourceI) {
                            continue;
                        }
                        // break on the first empty slot
                        else if (blockSlot.slot().empty()) {
                            break;
                        }
                        block[targetI] = blockSlot;
                        targetI += 1;
                    }

                    // there are no slots in this block so don't bother writing it
                    if (targetI == 0) {
                        continue;
                    }

                    nextBlocks[j] = block;
                }
            }

            var slotsToWrite = new LinkedArrayListSlot[SLOT_COUNT * 2];
            populateArray(slotsToWrite);
            int slotI = 0;

            // add the left block
            if (nextBlocks[0] != null) {
                for (LinkedArrayListSlot blockSlot : nextBlocks[0]) {
                    if (blockSlot.slot().empty()) {
                        break;
                    }
                    slotsToWrite[slotI] = blockSlot;
                    slotI += 1;
                }
            }

            // add the center block
            for (LinkedArrayListSlot slotMaybe : nextSlots) {
                if (slotMaybe != null) {
                    slotsToWrite[slotI] = slotMaybe;
                    slotI += 1;
                }
            }

            // add the right block
            if (nextBlocks[1] != null) {
                for (LinkedArrayListSlot blockSlot : nextBlocks[1]) {
                    if (blockSlot.slot().empty()) {
                        break;
                    }
                    slotsToWrite[slotI] = blockSlot;
                    slotI += 1;
                }
            }

            // clear the next slots
            nextSlots = new LinkedArrayListSlot[]{ null, null };

            // put the slots to write in separate blocks
            var blocks = new LinkedArrayListSlot[2][SLOT_COUNT];
            populateArray(blocks[0]);
            populateArray(blocks[1]);
            if (slotI > SLOT_COUNT) {
                // if there are enough slots to fill two blocks,
                // we need to decide which block to leave the gap in.
                // if the first list is smaller, leave the gap in
                // the first block. otherwise, leave it in the second.
                // this will cause the gap to stay near the left or
                // right edge of the concatenated list. we do this
                // because if many gaps form inside the list, the
                // branches will get long and lead to MaxShiftExceeded.
                if (headerA.size() < headerB.size()) {
                    for (int j = 0; j < slotI - SLOT_COUNT; j++) {
                        blocks[0][j] = slotsToWrite[j];
                    }
                    for (int j = 0; j < SLOT_COUNT; j++) {
                        blocks[1][j] = slotsToWrite[j + (slotI - SLOT_COUNT)];
                    }
                } else {
                    for (int j = 0; j < SLOT_COUNT; j++) {
                        blocks[0][j] = slotsToWrite[j];
                    }
                    for (int j = 0; j < slotI - SLOT_COUNT; j++) {
                        blocks[1][j] = slotsToWrite[j + SLOT_COUNT];
                    }
                }
            } else {
                for (int j = 0; j < slotI; j++) {
                    blocks[0][j] = slotsToWrite[j];
                }
            }

            // write the block(s)
            this.core.seek(this.core.length());
            for (int blockI = 0; blockI < blocks.length; blockI++) {
                var block = blocks[blockI];

                // this block is empty so don't bother writing it
                if (block[0].slot().empty()) {
                    break;
                }

                // write the block
                var nextPtr = this.core.position();
                long leafCount = 0;
                for (LinkedArrayListSlot blockSlot : block) {
                    writer.write(blockSlot.toBytes());
                    if (isLeafNode) {
                        if (!blockSlot.slot().empty()) {
                            leafCount += 1;
                        }
                    } else {
                        leafCount += blockSlot.size();
                    }
                }

                nextSlots[blockI] = new LinkedArrayListSlot(leafCount, new Slot(nextPtr, Tag.INDEX, true));
            }
        }

        long rootPtr;
        if (nextSlots[0] != null) {
            // if there is more than one slot, make a root node
            if (nextSlots[1] != null) {
                var block = new LinkedArrayListSlot[SLOT_COUNT];
                populateArray(block);
                block[0] = nextSlots[0];
                block[1] = nextSlots[1];

                // write the root node
                var newPtr = this.core.length();
                for (LinkedArrayListSlot blockSlot : block) {
                    writer.write(blockSlot.toBytes());
                }

                if (nextShift == MAX_BRANCH_LENGTH) throw new MaxShiftExceededException();
                nextShift += 1;

                rootPtr = newPtr;
            }
            // otherwise the first slot is the root node
            else {
                rootPtr = nextSlots[0].slot().value();
            }
        }
        // lists were empty so just re-use existing empty block
        else {
            rootPtr = headerA.ptr();
        }

        return new LinkedArrayListHeader(
            nextShift,
            rootPtr,
            headerA.size() + headerB.size()
        );
    }

    // compaction helpers

    private static Slot remapSlot(Core sourceCore, Core targetCore, short hashSize, HashMap<Long, Long> offsetMap, Slot slot) throws Exception {
        switch (slot.tag()) {
            case NONE, UINT, INT, FLOAT, SHORT_BYTES -> {
                return slot;
            }
            case BYTES -> {
                var mapped = offsetMap.get(slot.value());
                if (mapped != null) return new Slot(mapped, slot.tag(), slot.full());
                var newOffset = remapBytes(sourceCore, targetCore, slot);
                offsetMap.put(slot.value(), newOffset);
                return new Slot(newOffset, slot.tag(), slot.full());
            }
            case INDEX -> {
                var mapped = offsetMap.get(slot.value());
                if (mapped != null) return new Slot(mapped, slot.tag(), slot.full());
                var newOffset = remapIndex(sourceCore, targetCore, hashSize, offsetMap, slot);
                offsetMap.put(slot.value(), newOffset);
                return new Slot(newOffset, slot.tag(), slot.full());
            }
            case ARRAY_LIST -> {
                var mapped = offsetMap.get(slot.value());
                if (mapped != null) return new Slot(mapped, slot.tag(), slot.full());
                var newOffset = remapArrayList(sourceCore, targetCore, hashSize, offsetMap, slot);
                offsetMap.put(slot.value(), newOffset);
                return new Slot(newOffset, slot.tag(), slot.full());
            }
            case LINKED_ARRAY_LIST -> {
                var mapped = offsetMap.get(slot.value());
                if (mapped != null) return new Slot(mapped, slot.tag(), slot.full());
                var newOffset = remapLinkedArrayList(sourceCore, targetCore, hashSize, offsetMap, slot);
                offsetMap.put(slot.value(), newOffset);
                return new Slot(newOffset, slot.tag(), slot.full());
            }
            case HASH_MAP, HASH_SET -> {
                var mapped = offsetMap.get(slot.value());
                if (mapped != null) return new Slot(mapped, slot.tag(), slot.full());
                var newOffset = remapHashMapOrSet(sourceCore, targetCore, hashSize, offsetMap, slot, false);
                offsetMap.put(slot.value(), newOffset);
                return new Slot(newOffset, slot.tag(), slot.full());
            }
            case COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                var mapped = offsetMap.get(slot.value());
                if (mapped != null) return new Slot(mapped, slot.tag(), slot.full());
                var newOffset = remapHashMapOrSet(sourceCore, targetCore, hashSize, offsetMap, slot, true);
                offsetMap.put(slot.value(), newOffset);
                return new Slot(newOffset, slot.tag(), slot.full());
            }
            case KV_PAIR -> {
                var mapped = offsetMap.get(slot.value());
                if (mapped != null) return new Slot(mapped, slot.tag(), slot.full());
                var newOffset = remapKvPair(sourceCore, targetCore, hashSize, offsetMap, slot);
                offsetMap.put(slot.value(), newOffset);
                return new Slot(newOffset, slot.tag(), slot.full());
            }
            default -> throw new UnexpectedTagException();
        }
    }

    private static long remapBytes(Core sourceCore, Core targetCore, Slot slot) throws IOException {
        sourceCore.seek(slot.value());
        var sourceReader = sourceCore.reader();
        var length = sourceReader.readLong();

        // total size: long length + bytes + optional 2-byte format_tag
        var formatTagSize = slot.full() ? 2 : 0;
        var totalPayload = length + formatTagSize;

        var newOffset = targetCore.length();
        targetCore.seek(newOffset);
        var targetWriter = targetCore.writer();
        targetWriter.writeLong(length);

        // copy bytes in chunks
        var remaining = totalPayload;
        while (remaining > 0) {
            var chunk = (int) Math.min(remaining, 4096);
            var buf = new byte[chunk];
            sourceReader.readFully(buf);
            targetWriter.write(buf);
            remaining -= chunk;
        }

        return newOffset;
    }

    private static long remapIndex(Core sourceCore, Core targetCore, short hashSize, HashMap<Long, Long> offsetMap, Slot slot) throws Exception {
        // read 144-byte block (16 slots)
        sourceCore.seek(slot.value());
        var sourceReader = sourceCore.reader();
        var blockBytes = new byte[INDEX_BLOCK_SIZE];
        sourceReader.readFully(blockBytes);

        // remap each slot
        var buffer = ByteBuffer.wrap(blockBytes);
        var remappedSlots = new Slot[SLOT_COUNT];
        for (int i = 0; i < SLOT_COUNT; i++) {
            var slotBytes = new byte[Slot.length];
            buffer.get(slotBytes);
            var childSlot = Slot.fromBytes(slotBytes);
            remappedSlots[i] = remapSlot(sourceCore, targetCore, hashSize, offsetMap, childSlot);
        }

        // write remapped block to target
        var newOffset = targetCore.length();
        targetCore.seek(newOffset);
        var targetWriter = targetCore.writer();
        for (var s : remappedSlots) {
            targetWriter.write(s.toBytes());
        }

        return newOffset;
    }

    private static long remapArrayList(Core sourceCore, Core targetCore, short hashSize, HashMap<Long, Long> offsetMap, Slot slot) throws Exception {
        // read ArrayListHeader (16 bytes)
        sourceCore.seek(slot.value());
        var sourceReader = sourceCore.reader();
        var headerBytes = new byte[ArrayListHeader.length];
        sourceReader.readFully(headerBytes);
        var header = ArrayListHeader.fromBytes(headerBytes);

        // remap root index block pointer via remapSlot as an .index slot
        var indexSlot = new Slot(header.ptr(), Tag.INDEX);
        var remappedIndex = remapSlot(sourceCore, targetCore, hashSize, offsetMap, indexSlot);

        // write new ArrayListHeader with remapped ptr
        var newOffset = targetCore.length();
        targetCore.seek(newOffset);
        var targetWriter = targetCore.writer();
        targetWriter.write(new ArrayListHeader(remappedIndex.value(), header.size()).toBytes());

        return newOffset;
    }

    private static long remapLinkedArrayList(Core sourceCore, Core targetCore, short hashSize, HashMap<Long, Long> offsetMap, Slot slot) throws Exception {
        // read LinkedArrayListHeader (17 bytes)
        sourceCore.seek(slot.value());
        var sourceReader = sourceCore.reader();
        var headerBytes = new byte[LinkedArrayListHeader.length];
        sourceReader.readFully(headerBytes);
        var header = LinkedArrayListHeader.fromBytes(headerBytes);

        // remap root block
        var remappedPtr = remapLinkedArrayListBlock(sourceCore, targetCore, hashSize, offsetMap, header.ptr());

        // write new header
        var newOffset = targetCore.length();
        targetCore.seek(newOffset);
        var targetWriter = targetCore.writer();
        targetWriter.write(new LinkedArrayListHeader(header.shift(), remappedPtr, header.size()).toBytes());

        return newOffset;
    }

    private static long remapLinkedArrayListBlock(Core sourceCore, Core targetCore, short hashSize, HashMap<Long, Long> offsetMap, long blockOffset) throws Exception {
        // dedup check
        var mapped = offsetMap.get(blockOffset);
        if (mapped != null) return mapped;

        // read 272-byte block (16 x LinkedArrayListSlot of 17 bytes)
        sourceCore.seek(blockOffset);
        var sourceReader = sourceCore.reader();
        var blockBytes = new byte[LINKED_ARRAY_LIST_INDEX_BLOCK_SIZE];
        sourceReader.readFully(blockBytes);

        // parse slots
        var buffer = ByteBuffer.wrap(blockBytes);
        var slots = new LinkedArrayListSlot[SLOT_COUNT];
        for (int i = 0; i < SLOT_COUNT; i++) {
            var slotBytes = new byte[LinkedArrayListSlot.length];
            buffer.get(slotBytes);
            slots[i] = LinkedArrayListSlot.fromBytes(slotBytes);
        }

        // remap each slot
        var remappedSlots = new LinkedArrayListSlot[SLOT_COUNT];
        for (int i = 0; i < SLOT_COUNT; i++) {
            var s = slots[i];
            if (s.slot().tag() == Tag.INDEX) {
                // index slots point to other 272-byte blocks, recurse on ourselves
                var remappedPtr = remapLinkedArrayListBlock(sourceCore, targetCore, hashSize, offsetMap, s.slot().value());
                remappedSlots[i] = new LinkedArrayListSlot(s.size(), new Slot(remappedPtr, Tag.INDEX, s.slot().full()));
            } else if (s.slot().empty()) {
                remappedSlots[i] = s;
            } else {
                // leaf slot - remap via remapSlot
                var remapped = remapSlot(sourceCore, targetCore, hashSize, offsetMap, s.slot());
                remappedSlots[i] = new LinkedArrayListSlot(s.size(), remapped);
            }
        }

        // write remapped block to target
        var newOffset = targetCore.length();
        targetCore.seek(newOffset);
        var targetWriter = targetCore.writer();
        for (var s : remappedSlots) {
            targetWriter.write(s.toBytes());
        }

        offsetMap.put(blockOffset, newOffset);
        return newOffset;
    }

    private static long remapHashMapOrSet(Core sourceCore, Core targetCore, short hashSize, HashMap<Long, Long> offsetMap, Slot slot, boolean counted) throws Exception {
        sourceCore.seek(slot.value());
        var sourceReader = sourceCore.reader();

        long countValue = -1;
        if (counted) {
            countValue = sourceReader.readLong();
        }

        // read 144-byte root index block
        var blockBytes = new byte[INDEX_BLOCK_SIZE];
        sourceReader.readFully(blockBytes);

        // remap each child slot in the block
        var buffer = ByteBuffer.wrap(blockBytes);
        var remappedSlots = new Slot[SLOT_COUNT];
        for (int i = 0; i < SLOT_COUNT; i++) {
            var slotBytes = new byte[Slot.length];
            buffer.get(slotBytes);
            var childSlot = Slot.fromBytes(slotBytes);
            remappedSlots[i] = remapSlot(sourceCore, targetCore, hashSize, offsetMap, childSlot);
        }

        // write [optional count][remapped block] contiguously to target
        var newOffset = targetCore.length();
        targetCore.seek(newOffset);
        var targetWriter = targetCore.writer();
        if (counted) {
            targetWriter.writeLong(countValue);
        }
        for (var s : remappedSlots) {
            targetWriter.write(s.toBytes());
        }

        return newOffset;
    }

    private static long remapKvPair(Core sourceCore, Core targetCore, short hashSize, HashMap<Long, Long> offsetMap, Slot slot) throws Exception {
        // read KeyValuePair
        sourceCore.seek(slot.value());
        var sourceReader = sourceCore.reader();
        var kvPairBytes = new byte[KeyValuePair.length(hashSize)];
        sourceReader.readFully(kvPairBytes);
        var kvPair = KeyValuePair.fromBytes(kvPairBytes, hashSize);

        // remap key_slot and value_slot
        var remappedKey = remapSlot(sourceCore, targetCore, hashSize, offsetMap, kvPair.keySlot());
        var remappedValue = remapSlot(sourceCore, targetCore, hashSize, offsetMap, kvPair.valueSlot());

        // write remapped KV pair (hash stays unchanged)
        var newOffset = targetCore.length();
        targetCore.seek(newOffset);
        var targetWriter = targetCore.writer();
        targetWriter.write(new KeyValuePair(remappedValue, remappedKey, kvPair.hash()).toBytes());

        return newOffset;
    }
}
