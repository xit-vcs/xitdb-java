package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Objects;

public class Database {
    public Core core;
    private final MessageDigest md;
    public volatile Header header;
    public Long txStart;
    volatile Transaction transaction;

    // for file IDs and commit IDs
    private static final SecureRandom RANDOM = new SecureRandom();

    static final class Transaction {
        final Thread thread = Thread.currentThread();
        volatile boolean aborted;
        Long rootPosition;
        // the copy of the root slot that the transaction works on
        Long scratchPosition;
        Long frozenAt;
    }

    public static final short VERSION = 1;
    public static final byte[] MAGIC_NUMBER = new byte[]{'x', 'i', 't'};
    // the database header is followed by the root header. the top-level data
    // is a root slot of any type, and every write to it is a copy-on-write
    // transaction that appends a trailer holding the new root slot. the root
    // header only caches the latest trailer, so every other committed byte is
    // immutable, and a replica stays in sync by copying the bytes it doesn't have.
    public static final int ROOT_HEADER_START = Header.length;
    public static final int ROOT_HEADER_END = ROOT_HEADER_START + RootHeader.length;
    public static final int BIT_COUNT = 4;
    public static final int SLOT_COUNT = 1 << BIT_COUNT;
    public static final long MASK = SLOT_COUNT - 1;
    public static final BigInteger BIG_MASK = BigInteger.valueOf(MASK);
    public static final int INDEX_BLOCK_SIZE = Slot.length * SLOT_COUNT;
    public static final int MAX_BRANCH_LENGTH = 16;

    // b-tree (backs the linked_array_list, and the sorted_map/sorted_set)
    public static final int BTREE_SLOT_COUNT = SLOT_COUNT; // max entries per leaf / children per branch
    public static final int BTREE_SPLIT_COUNT = (BTREE_SLOT_COUNT + 1) / 2; // left side of a split
    // on-disk node block: [kind: u8][num: u8] followed by, for a leaf,
    // BTREE_SLOT_COUNT value slots; for a branch, BTREE_SLOT_COUNT child slots
    // then BTREE_SLOT_COUNT u64 subtree counts
    public static final int BTREE_NODE_HEADER_SIZE = 2;
    public static final int BTREE_LEAF_BLOCK_SIZE = BTREE_NODE_HEADER_SIZE + Slot.length * BTREE_SLOT_COUNT;
    public static final int BTREE_BRANCH_BLOCK_SIZE = BTREE_NODE_HEADER_SIZE + (Slot.length + 8) * BTREE_SLOT_COUNT;
    // sorted_map / sorted_set node block: [kind: u8][num: u8] followed by, for a
    // leaf, BTREE_SLOT_COUNT .sorted_kv_pair slots; for a branch, BTREE_SLOT_COUNT
    // child slots, then BTREE_SLOT_COUNT separator slots, then BTREE_SLOT_COUNT
    // u64 counts
    public static final int SORTED_LEAF_BLOCK_SIZE = BTREE_NODE_HEADER_SIZE + Slot.length * BTREE_SLOT_COUNT;
    public static final int SORTED_BRANCH_BLOCK_SIZE = BTREE_NODE_HEADER_SIZE + (Slot.length * 2 + 8) * BTREE_SLOT_COUNT;

    public static enum WriteMode {
        READ_ONLY,
        READ_WRITE
    }

    // init

    public Database(Core core, Hasher hasher) throws IOException {
        this(core, hasher, null);
    }

    // fileId is the file ID of a new database. if null, a random one is used.
    public Database(Core core, Hasher hasher, Long fileId) throws IOException {
        this.core = core;
        this.md = newDigest(hasher.md());
        this.txStart = null;

        core.seek(0);
        if (core.length() == 0) {
            this.header = new Header(fileId != null ? fileId : RANDOM.nextLong(), hasher.id(), (short)this.md.getDigestLength(), VERSION, MAGIC_NUMBER);
            this.header.write(core);
            var writer = core.writer();
            // write the root header and commit an empty root, so the
            // file ends with a trailer like after any transaction
            writer.write(new RootHeader(0, new Slot()).toBytes());
            // the first commit's ID is 0, so a new database's bytes only
            // depend on its file ID
            commitRoot(new Slot(), 0);
            this.core.sync();
        } else {
            this.header = readAndValidateHeader();
            committedSize();
        }
    }

    // TODO: consider a per-thread digest pool to reduce allocation
    private static MessageDigest newDigest(MessageDigest source) {
        try {
            var digest = (MessageDigest)source.clone();
            digest.reset();
            return digest;
        } catch (CloneNotSupportedException e) {
            try {
                return MessageDigest.getInstance(source.getAlgorithm(), source.getProvider());
            } catch (NoSuchAlgorithmException | IllegalArgumentException cause) {
                throw new IllegalStateException("Cannot create an independent digest", cause);
            }
        }
    }

    public byte[] hash(byte[] bytes) {
        return newDigest(this.md).digest(bytes);
    }

    // returns an independent copy of the hash configuration
    public Hasher hasher() {
        return new Hasher(newDigest(this.md), this.header.hashId());
    }

    @FunctionalInterface
    public interface HashFunction {
        void update(MessageDigest digest) throws Exception;
    }

    // the callback's digest is private to this operation
    public byte[] hash(HashFunction fn) throws Exception {
        var digest = newDigest(this.md);
        fn.update(digest);
        return digest.digest();
    }

    private Header readAndValidateHeader() throws IOException {
        core.seek(0);
        var header = Header.read(core);
        header.validate();
        if (header.hashSize() != this.md.getDigestLength()) {
            throw new InvalidHashSizeException();
        }
        return header;
    }

    public WriteCursor rootCursor() throws IOException {
        // the cursor only holds a snapshot of the root slot, and every
        // operation that starts at it reads the root again. if this read
        // fails, the cursor looks empty and the next operation throws the exception.
        Slot root;
        try {
            root = readRootSlot();
        } catch (IOException | DatabaseException e) {
            root = new Slot();
        }
        return new WriteCursor(new SlotPointer((long) ROOT_HEADER_START, root), this);
    }

    /**
     * runs fn with a write cursor to the root in a single transaction,
     * discarding everything it wrote if it throws. use it to group writes to
     * a root that isn't an array list, such as a hash map. for an array list
     * root, WriteArrayList.appendContext does the same.
     */
    public void runContext(ContextFunction fn) throws Exception {
        rootCursor().writePath(new PathPart[]{new Context(fn)});
    }

    public void freeze() throws IOException {
        var active = this.transaction;
        if (active == null) {
            throw new ExpectedTxStartException();
        }
        if (active.thread != Thread.currentThread()) {
            throw new IllegalStateException("Transaction belongs to another thread");
        }
        if (this.txStart == null) throw new ExpectedTxStartException();
        this.txStart = this.core.length();
        active.frozenAt = this.txStart;
    }

    public Database compact(Core targetCore) throws Exception {
        return compact(targetCore, new OffsetMap() {
            private final HashMap<Long, Long> offsets = new HashMap<>();

            @Override
            public void reset() {
                offsets.clear();
            }

            @Override
            public Long get(long sourceOffset) {
                return offsets.get(sourceOffset);
            }

            @Override
            public void put(long sourceOffset, long targetOffset) {
                offsets.put(sourceOffset, targetOffset);
            }
        });
    }

    /**
     * resets and borrows the supplied offsets map without closing it.
     *
     * the target must be a new or empty database, or this throws
     * `TargetNotEmptyException`.
     */
    public Database compact(Core targetCore, OffsetMap offsetMap) throws Exception {
        // cached offsets only apply to this compaction's target
        offsetMap.reset();
        var hasher = new Hasher(this.md, this.header.hashId());
        var target = new Database(targetCore, hasher);

        // the target must be a new or empty database, because compaction
        // writes to it with ordinary transactions. this also rejects
        // compacting a database into its own file.
        if (!target.readRootSlot().empty()) throw new TargetNotEmptyException();

        var sourceRoot = readRootSlot();

        // an empty root compacts to a new database. a root explicitly
        // set to null isn't empty, so it is remapped below.
        if (sourceRoot.empty()) return target;

        // the target is written as one ordinary transaction, so it is committed
        // and fsynced like any other (callers typically rename it over an
        // existing database file). the remap must run inside the transaction,
        // because starting one truncates the target.
        var compactor = new Compactor(this.core, target.core, this.header.hashSize(), offsetMap);

        switch (sourceRoot.tag()) {
            // an array list root is a history, and its latest moment
            // becomes the only moment of a new one
            case ARRAY_LIST -> {
                var lastMoment = this.rootCursor().readPathSlot(new PathPart[]{new ArrayListGet(-1)});
                if (lastMoment != null) {
                    target.rootCursor().writePath(new PathPart[]{
                        new ArrayListInit(),
                        new ArrayListAppend(),
                        new Context(cursor -> cursor.write(compactor.remapSlot(lastMoment))),
                    });
                }
            }
            // any other root is remapped whole
            default -> target.runContext(cursor -> cursor.write(compactor.remapSlot(sourceRoot)));
        }

        return target;
    }

    /** this database's ReplicaState. */
    public ReplicaState replicaState() throws IOException {
        var committedSize = committedSize();
        return new ReplicaState(committedSize, readRootTrailer(committedSize).checksum());
    }

    /**
     * where to start sending committed bytes to a replica in state. that's its
     * committed size if its bytes are a prefix of this database, or 0 if it must
     * be replaced, because it's a replica of another file or it has diverged
     * from this one.
     */
    public long replicationStart(ReplicaState state) throws IOException {
        if (state.committedSize() > committedSize()) return 0;
        RootTrailer trailer;
        try {
            trailer = readRootTrailer(state.committedSize());
        } catch (InvalidDatabaseException | UnexpectedTagException e) {
            return 0;
        }
        return trailer.checksum() == state.checksum() ? state.committedSize() : 0;
    }

    /**
     * writes the committed bytes from start up to end to writer. start is
     * usually from replicationStart, and end can't be past committedSize. committed bytes never change except for
     * the root header, so when start is 0, a root header matching end is
     * written, making the bytes a complete database by themselves.
     */
    public void readCommitted(long start, long end, OutputStream writer) throws IOException {
        try (var reader = openCommitted(start, end)) {
            reader.transferTo(writer);
        }
    }

    /**
     * like readCommitted, but returns a stream that reads the bytes on demand instead of writing them all to an OutputStream.
     * committed bytes never change, so the stream stays valid while this
     * database keeps committing. it shares this database's core, so only read
     * it while writes are happening if the core supports concurrent reads.
     */
    public InputStream openCommitted(long start, long end) throws IOException {
        if (start < 0 || start > end || end > committedSize()) throw new InvalidRangeException();
        if (start > 0 && start < ROOT_HEADER_END) throw new InvalidRangeException();

        // the root header on disk may already be newer than end, so a stream
        // starting at 0 substitutes one that matches it
        var rootHeader = start == 0 ? new RootHeader(end, readRootTrailer(end).root()).toBytes() : null;

        return new InputStream() {
            long pos = start;

            @Override
            public int read(byte[] buffer, int off, int len) throws IOException {
                Objects.checkFromIndexSize(off, len, buffer.length);
                if (len == 0) return 0;
                if (pos >= end) return -1;
                var n = (int) Math.min(len, end - pos);
                if (rootHeader != null && pos >= ROOT_HEADER_START && pos < ROOT_HEADER_END) {
                    n = (int) Math.min(n, ROOT_HEADER_END - pos);
                    System.arraycopy(rootHeader, (int) (pos - ROOT_HEADER_START), buffer, off, n);
                } else {
                    if (rootHeader != null && pos < ROOT_HEADER_START) n = (int) Math.min(n, ROOT_HEADER_START - pos);
                    // seek every time, because other reads share the core
                    core.seek(pos);
                    core.reader().readFully(buffer, off, n);
                }
                pos += n;
                return n;
            }

            @Override
            public int read() throws IOException {
                var buffer = new byte[1];
                return read(buffer, 0, 1) == -1 ? -1 : buffer[0] & 0xff;
            }
        };
    }

    /**
     * writes len bytes from reader at start and commits them. they must be the bytes from readCommitted, so they end
     * with a trailer. when start is 0, this database is replaced by them;
     * otherwise start must be this database's committedSize.
     */
    public void applyReplicated(long start, InputStream reader, long len) throws IOException {
        // if reader runs out early, close throws too, and try-with-resources
        // keeps the original exception with close's as suppressed
        try (var writer = openReplicated(start, len)) {
            var buffer = new byte[4096];
            var remaining = len;
            while (remaining > 0) {
                var n = reader.readNBytes(buffer, 0, (int) Math.min(remaining, buffer.length));
                if (n == 0) throw new EndOfStreamException();
                writer.write(buffer, 0, n);
                remaining -= n;
            }
        }
    }

    /**
     * like applyReplicated, but returns a stream to write the len bytes to. they are written to this database as they arrive, and
     * close commits them. if fewer than len bytes were written, or they don't end
     * with a trailer, close throws and the database stays at its last commit.
     * when start is 0, this database is truncated now and replaced by the bytes.
     */
    public OutputStream openReplicated(long start, long len) throws IOException {
        if (len < 0) throw new InvalidRangeException();
        if (start == 0) {
            this.core.setLength(0);
        } else if (start == committedSize()) {
            // discard anything past the committed bytes, such as the remains of an interrupted sync
            this.truncate();
        } else {
            throw new InvalidRangeException();
        }

        return new OutputStream() {
            long written = 0;
            boolean closed = false;

            @Override
            public void write(byte[] buffer, int off, int n) throws IOException {
                Objects.checkFromIndexSize(off, n, buffer.length);
                if (closed) throw new IOException("stream closed");
                if (n > len - written) throw new InvalidRangeException();
                core.seek(start + written);
                // not every core implements write(byte[], int, int)
                core.writer().write(off == 0 && n == buffer.length ? buffer : Arrays.copyOfRange(buffer, off, off + n));
                written += n;
            }

            @Override
            public void write(int b) throws IOException {
                write(new byte[] {(byte) b}, 0, 1);
            }

            @Override
            public void close() throws IOException {
                if (closed) return;
                closed = true;
                if (written != len) throw new EndOfStreamException();
                commitReplicated(start, len);
            }
        };
    }

    // commits len bytes written at start by a replication stream
    private void commitReplicated(long start, long len) throws IOException {
        if (start == 0) {
            // the bytes are a different database, with its own header
            this.core.sync();
            this.header = readAndValidateHeader();
            committedSize();
        } else if (len > 0) {
            // the bytes must end with a trailer. update the root header to match it.
            var trailer = readRootTrailer(start + len);
            this.core.sync();
            writeRootHeader(trailer.root());
            this.core.sync();
        }
    }

    // private

    /**
     * the number of bytes that belong to committed transactions. they never
     * change except for the root header (see readCommitted).
     */
    public long committedSize() throws IOException {
        var rootHeader = readRootHeader();
        var minimumSize = ROOT_HEADER_END + RootTrailer.length;
        if (rootHeader.fileSize() < minimumSize) throw new InvalidDatabaseException();
        if (this.core.length() < rootHeader.fileSize()) throw new TruncatedDatabaseException();
        // the committed bytes must end with a trailer matching the header
        if (!readRootTrailer(rootHeader.fileSize()).root().equals(rootHeader.root())) {
            throw new InvalidDatabaseException();
        }
        return rootHeader.fileSize();
    }

    void truncate() throws IOException {
        var committedSize = committedSize();
        var active = this.transaction;
        if (active != null && active.frozenAt != null && active.frozenAt > committedSize) {
            // retain frozen bytes without publishing the failed transaction.
            // commit the unchanged root again after them, so the committed
            // bytes still end with a trailer
            if (this.core.length() > active.frozenAt) {
                this.core.setLength(active.frozenAt);
            }
            commitRoot(readRootSlot(), RANDOM.nextLong());
            this.core.sync();
            committedSize = active.frozenAt + RootTrailer.length;
        }
        if (this.core.length() > committedSize) {
            this.core.setLength(committedSize);
        }
    }

    RootHeader readRootHeader() throws IOException {
        this.core.seek(ROOT_HEADER_START);
        var bytes = new byte[RootHeader.length];
        this.core.reader().readFully(bytes);
        return RootHeader.fromBytes(bytes);
    }

    // the most recently committed root slot
    Slot readRootSlot() throws IOException {
        return readRootHeader().root();
    }

    // the trailer that ends at end
    private RootTrailer readRootTrailer(long end) throws IOException {
        if (end < RootTrailer.length) throw new InvalidDatabaseException();
        var trailerPos = end - RootTrailer.length;
        this.core.seek(trailerPos);
        var bytes = new byte[RootTrailer.length];
        this.core.reader().readFully(bytes);
        var rootBytes = Arrays.copyOfRange(bytes, 0, Slot.length);
        var commitId = ByteBuffer.wrap(bytes, Slot.length, 8).getLong();
        var checksum = ByteBuffer.wrap(bytes, Slot.length + 8, 8).getLong();
        if (checksum != RootTrailer.computeChecksum(this.header.fileId(), trailerPos, rootBytes, commitId)) throw new InvalidDatabaseException();
        return new RootTrailer(checksum, commitId, Slot.fromBytes(rootBytes));
    }

    // completes a transaction by appending a trailer for root and updating
    // the root header. callers must sync afterwards to make the header itself
    // durable.
    private void commitRoot(Slot root, long commitId) throws IOException {
        var trailerPos = this.core.length();
        this.core.seek(trailerPos);
        this.core.writer().write(RootTrailer.init(this.header.fileId(), trailerPos, root, commitId).toBytes());

        // flush and fsync before updating the header, because updating
        // the header is what completes the transaction. without the
        // fsync, the OS could persist the header before the data it
        // points to, so a crash could commit a transaction whose data
        // never reached disk.
        this.core.sync();

        writeRootHeader(root);
    }

    // points the root header at root, which must be in the trailer that
    // ends the file.
    private void writeRootHeader(Slot root) throws IOException {
        var fileSize = this.core.length();
        this.core.seek(ROOT_HEADER_START);
        this.core.writer().write(new RootHeader(fileSize, root).toBytes());
    }

    // whether this slot pointer is the root of the database
    static boolean isRoot(SlotPointer slotPtr) {
        return slotPtr.position() != null && slotPtr.position() == ROOT_HEADER_START;
    }

    // the collection tag that an init path part creates, if any
    private static Tag initTag(PathPart part) {
        if (part instanceof ArrayListInit) {
            return Tag.ARRAY_LIST;
        } else if (part instanceof LinkedArrayListInit) {
            return Tag.LINKED_ARRAY_LIST;
        } else if (part instanceof HashMapInit init) {
            return init.counted()
                ? (init.set() ? Tag.COUNTED_HASH_SET : Tag.COUNTED_HASH_MAP)
                : (init.set() ? Tag.HASH_SET : Tag.HASH_MAP);
        } else if (part instanceof SortedMapInit init) {
            return init.set() ? Tag.SORTED_SET : Tag.SORTED_MAP;
        } else {
            return null;
        }
    }

    // the init path part for a collection tag, if any
    private static PathPart initPartFor(Tag tag) {
        return switch (tag) {
            case ARRAY_LIST -> new ArrayListInit();
            case LINKED_ARRAY_LIST -> new LinkedArrayListInit();
            case HASH_MAP -> new HashMapInit(false, false);
            case HASH_SET -> new HashMapInit(false, true);
            case COUNTED_HASH_MAP -> new HashMapInit(true, false);
            case COUNTED_HASH_SET -> new HashMapInit(true, true);
            case SORTED_MAP -> new SortedMapInit(false);
            case SORTED_SET -> new SortedMapInit(true);
            default -> null;
        };
    }

    // whether the data at ptr predates this transaction, so it must be
    // copied before it is modified. writes only happen inside a transaction.
    private boolean isOld(long ptr) {
        if (this.txStart == null) throw new ExpectedTxStartException();
        return ptr < this.txStart;
    }

    // copies an index block to the end unless it was made in this transaction
    private long copyIndexBlockIfOld(long ptr) throws IOException {
        if (!isOld(ptr)) return ptr;
        var indexBlock = new byte[INDEX_BLOCK_SIZE];
        this.core.seek(ptr);
        this.core.reader().readFully(indexBlock);
        var nextPtr = this.core.length();
        this.core.seek(nextPtr);
        this.core.writer().write(indexBlock);
        return nextPtr;
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

    // reject writes into frozen data; the current transaction's root slot remains writable
    void checkFrozenSlot(SlotPointer slotPtr) {
        var active = this.transaction;
        if (active != null && active.frozenAt != null && slotPtr.position() != null
            && slotPtr.position() < active.frozenAt && !slotPtr.position().equals(active.rootPosition)) {
            throw new IllegalStateException("Writer points into frozen data; reacquire it from the transaction root");
        }
    }

    // copy frozen collection storage before mutation to preserve existing readers
    private SlotPointer copyCollectionIfFrozen(SlotPointer slotPtr, PathPart init) throws Exception {
        if (this.transaction != null
            && this.transaction.frozenAt != null && slotPtr.slot().value() < this.transaction.frozenAt) {
            return init.readSlotPointer(this, WriteMode.READ_WRITE, new PathPart[]{init}, 0, slotPtr);
        }
        return slotPtr;
    }

    protected SlotPointer readSlotPointer(WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
        if (pathI == path.length) {
            if (writeMode == WriteMode.READ_ONLY && slotPtr.slot().empty()) {
                throw new KeyNotFoundException();
            }
            return slotPtr;
        }
        var part = path[pathI];

        // every write that starts at the root is a transaction
        if (isRoot(slotPtr)) {
            var root = readRootSlot();

            // the position of a read path doesn't matter, but it must not be
            // ROOT_HEADER_START or this would recurse forever
            if (writeMode == WriteMode.READ_ONLY) {
                return readSlotPointer(writeMode, path, pathI, new SlotPointer(null, root));
            }

            // initializing an initialized root is free, so a wrapper like
            // WriteArrayList can hold a root cursor without a transaction. like
            // starting one, it discards data left by an unfinished transaction.
            // (writePath rejects root writes inside a transaction, so none
            // is open here.)
            if (path.length - pathI == 1 && root.tag() != Tag.NONE) {
                var tag = initTag(part);
                if (tag != null && tag == root.tag()) {
                    this.truncate();
                    return new SlotPointer((long) ROOT_HEADER_START, root);
                }
            }

            // discard data left by an unfinished transaction after a crash
            this.truncate();
            var scratchPos = this.core.length();
            this.txStart = scratchPos;
            try {
                // the transaction works on a copy of the root slot
                this.core.seek(scratchPos);
                this.core.writer().write(root.toBytes());
                var rootPtr = new SlotPointer(scratchPos, root);
                // the copy is the transaction's root slot, so it stays writable after a freeze
                if (this.transaction != null) {
                    this.transaction.rootPosition = scratchPos;
                    this.transaction.scratchPosition = scratchPos;
                }

                // every collection expects its init part to copy its header or root
                // block into the transaction before anything writes to it. the root
                // has no enclosing init part, so run one here.
                var initPart = initPartFor(root.tag());
                if (initPart != null) {
                    rootPtr = readSlotPointer(writeMode, new PathPart[]{initPart}, 0, rootPtr);
                }

                var result = readSlotPointer(writeMode, path, pathI, rootPtr);

                // the copy now holds the new root, so commit it
                this.core.seek(scratchPos);
                var rootBytes = new byte[Slot.length];
                this.core.reader().readFully(rootBytes);
                var nextRoot = Slot.fromBytes(rootBytes);
                this.commitRoot(nextRoot, RANDOM.nextLong());

                // a path that ends at the root (such as an init or a write) returns
                // a root cursor, so it can be used for later transactions
                if (result.position() != null && result.position() == scratchPos) {
                    result = new SlotPointer((long) ROOT_HEADER_START, nextRoot);
                }
                return result;
            } finally {
                this.txStart = null;
            }
        }

        if (writeMode == WriteMode.READ_WRITE) checkFrozenSlot(slotPtr);

        return part.readSlotPointer(this, writeMode, path, pathI, slotPtr);
    }

    // records

    public static record Header (
        // a random ID that never changes. it is part of every trailer's checksum,
        // so a replica can't be extended with bytes from a different file.
        long fileId,
        int hashId,
        // the size in bytes of all hashes used by the database.
        short hashSize,
        // the file format version
        short version,
        byte[] magicNumber
    ) {
        public static int length = 20;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.magicNumber);
            // currently unused
            buffer.put((byte)0);
            buffer.putShort(this.version);
            buffer.putShort(this.hashSize);
            buffer.putInt(this.hashId);
            buffer.putLong(this.fileId);
            return buffer.array();
        }

        public static Header read(Core core) throws IOException {
            var reader = core.reader();
            var magicNumber = new byte[3];
            reader.readFully(magicNumber);
            // currently unused
            reader.readByte();
            var version = reader.readShort();
            var hashSize = reader.readShort();
            var hashId = reader.readInt();
            var fileId = reader.readLong();
            return new Header(fileId, hashId, hashSize, version, magicNumber);
        }

        public void write(Core core) throws IOException {
            var writer = core.writer();
            writer.write(this.toBytes());
        }

        public void validate() {
            if (!Arrays.equals(this.magicNumber, MAGIC_NUMBER)) {
                throw new InvalidDatabaseException();
            }
            if (this.version != VERSION) {
                throw new InvalidVersionException();
            }
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

    // rewritten at every commit, as a cache of the latest
    // trailer. it is written big endian, so the root slot comes first on disk.
    public static record RootHeader(long fileSize, Slot root) {
        public static final int length = Slot.length + 8;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.root.toBytes());
            buffer.putLong(this.fileSize);
            return buffer.array();
        }

        public static RootHeader fromBytes(byte[] bytes) {
            var buffer = ByteBuffer.wrap(bytes);
            var rootBytes = new byte[Slot.length];
            buffer.get(rootBytes);
            var root = Slot.fromBytes(rootBytes);
            var fileSize = buffer.getLong();
            return new RootHeader(fileSize, root);
        }
    }

    // appended at the end of every transaction, so the
    // committed bytes (up to the root header's fileSize) always end with one
    // it is written big endian, so root comes first on disk.
    public static record RootTrailer(
        long checksum,
        // random for every transaction except the first, so separately written
        // copies of a file can't end up with the same trailers, even when they
        // write data of the same size. that is how a replica notices it is being
        // sent bytes from a different history.
        long commitId,
        Slot root
    ) {
        public static final int length = Slot.length + 16;

        public static RootTrailer init(long fileId, long position, Slot root, long commitId) {
            return new RootTrailer(computeChecksum(fileId, position, root.toBytes(), commitId), commitId, root);
        }

        // CRC-64/XZ of the file ID, the trailer's position, root, and commitId,
        // all big endian. including the file ID and position means the trailer
        // can't be mistaken for one written elsewhere. 64 bits makes it safe to
        // find the last trailer by scanning a file whose root header can't be trusted.
        public static long computeChecksum(long fileId, long position, byte[] rootBytes, long commitId) {
            var buffer = ByteBuffer.allocate(24 + Slot.length);
            buffer.putLong(fileId);
            buffer.putLong(position);
            buffer.put(rootBytes);
            buffer.putLong(commitId);
            return Crc64Xz.hash(buffer.array());
        }

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.root.toBytes());
            buffer.putLong(this.commitId);
            buffer.putLong(this.checksum);
            return buffer.array();
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

    // a KeyValuePair without the hash, which sorted maps don't need
    public static record SortedKeyValuePair(Slot valueSlot, Slot keySlot) {
        public static int length = Slot.length * 2;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(keySlot.toBytes());
            buffer.put(valueSlot.toBytes());
            return buffer.array();
        }

        public static SortedKeyValuePair fromBytes(byte[] bytes) {
            var buffer = ByteBuffer.wrap(bytes);
            var keySlotBytes = new byte[Slot.length];
            buffer.get(keySlotBytes);
            var keySlot = Slot.fromBytes(keySlotBytes);
            var valueSlotBytes = new byte[Slot.length];
            buffer.get(valueSlotBytes);
            var valueSlot = Slot.fromBytes(valueSlotBytes);
            return new SortedKeyValuePair(valueSlot, keySlot);
        }
    }

    // where the key slot is in a .kv_pair or .sorted_kv_pair entry. the value slot follows it.
    long kvKeyOffset(Tag tag) {
        return tag == Tag.KV_PAIR ? this.header.hashSize() : 0;
    }

    // header for both B+trees (the positional linked_array_list and the sorted
    // sorted_map/sorted_set): a root pointer plus the element count
    public static record BTreeHeader(long rootPtr, long size) {
        public static int length = 16;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.putLong(this.size);
            buffer.putLong(this.rootPtr);
            return buffer.array();
        }

        public static BTreeHeader fromBytes(byte[] bytes) {
            var buffer = ByteBuffer.wrap(bytes);
            var size = checkLong(buffer.getLong());
            var rootPtr = checkLong(buffer.getLong());
            return new BTreeHeader(rootPtr, size);
        }
    }

    public static enum BTreeNodeKind { LEAF, BRANCH }

    public static final class BTreeNode {
        public BTreeNodeKind kind;
        public int num;
        public Slot[] values = new Slot[BTREE_SLOT_COUNT];   // leaf
        public Slot[] children = new Slot[BTREE_SLOT_COUNT]; // branch
        public long[] counts = new long[BTREE_SLOT_COUNT];   // branch

        public BTreeNode(BTreeNodeKind kind, int num) {
            this.kind = kind;
            this.num = num;
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                this.values[i] = new Slot();
                this.children[i] = new Slot();
            }
        }

        public long subtreeCount() {
            if (this.kind == BTreeNodeKind.LEAF) return this.num;
            long total = 0;
            for (int i = 0; i < this.num; i++) total += this.counts[i];
            return total;
        }
    }

    // a node pointer plus the element count of its subtree (the right sibling of a split)
    public static record BTreeNodeRef(long nodePtr, long count) {}

    public static record BTreeInsertResult(long nodePtr, long count, long valuePosition, BTreeNodeRef split) {}

    public static record BTreeWriteSlot(long nodePtr, long valuePosition, Slot slot) {}

    public static record BTreeJoinResult(long nodePtr, long count, BTreeNodeRef split) {}

    public static record BTreeSplitResult(long left, long right) {}

    // sorted_map / sorted_set: a count-augmented B+tree keyed on arbitrary byte
    // strings, ordered lexicographically. reuses the b-tree's capacity constants,
    // persistence model (txStart reuse), and the BTreeHeader {rootPtr, size}
    // header. a leaf holds .sorted_kv_pair entries in ascending key order;
    // a branch holds child slots, separator slots (the smallest key in each child's
    // subtree; separators[0] is an unused sentinel), and per-child subtree counts.
    public static final class SortedNode {
        public BTreeNodeKind kind;
        public int num;
        public Slot[] entries = new Slot[BTREE_SLOT_COUNT];    // leaf
        public Slot[] children = new Slot[BTREE_SLOT_COUNT];   // branch
        public Slot[] separators = new Slot[BTREE_SLOT_COUNT]; // branch
        public long[] counts = new long[BTREE_SLOT_COUNT];     // branch

        public SortedNode(BTreeNodeKind kind, int num) {
            this.kind = kind;
            this.num = num;
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                this.entries[i] = new Slot();
                this.children[i] = new Slot();
                this.separators[i] = new Slot();
            }
        }

        public long subtreeCount() {
            if (this.kind == BTreeNodeKind.LEAF) return this.num;
            long total = 0;
            for (int i = 0; i < this.num; i++) total += this.counts[i];
            return total;
        }
    }

    // the new right sibling produced when a node splits
    public static record SortedSplit(long nodePtr, long count, Slot separator) {}

    // insert/replace result: the entry whose value to write, whether a new entry
    // was added (vs replacing), and the new right sibling if this node split
    public static record SortedInsertResult(long nodePtr, long count, Slot kvSlot, boolean added, SortedSplit split) {}

    // remove result threaded back up the descent: the rewritten node and whether the
    // key was found. separators are stable lower-bound boundaries (not exact mins), so
    // deletions never refresh them; an emptied leaf is left in place.
    public static record SortedRemoveResult(long nodePtr, boolean found) {}

    public static record SortedSlot(Slot slot, long position) {}

    public static record SortedEntry(Slot kvSlot, Slot keySlot) {}

    public static sealed interface PathPart permits ArrayListInit, ArrayListGet, ArrayListAppend, ArrayListSlice, LinkedArrayListInit, LinkedArrayListGet, LinkedArrayListAppend, LinkedArrayListSlice, LinkedArrayListConcat, LinkedArrayListInsert, LinkedArrayListRemove, HashMapInit, HashMapGet, HashMapRemove, SortedMapInit, SortedMapGet, SortedMapGetIndex, SortedMapRemove, WriteData, Context {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception;
    }

    public static record ArrayListInit() implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

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
                    if (db.isOld(arrayListStart)) {
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
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case ARRAY_LIST -> {}
                default -> throw new UnexpectedTagException();
            }

            if (writeMode == WriteMode.READ_WRITE) slotPtr = db.copyCollectionIfFrozen(slotPtr, new ArrayListInit());
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
            var finalSlotPtr = db.readArrayListSlot(header.ptr, key, shift, writeMode);

            return db.readSlotPointer(writeMode, path, pathI + 1, finalSlotPtr);
        }
    }

    public static record ArrayListAppend() implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            slotPtr = db.copyCollectionIfFrozen(slotPtr, new ArrayListInit());
            var nextArrayListStart = slotPtr.slot().value();

            // read header
            db.core.seek(nextArrayListStart);
            var headerBytes = new byte[ArrayListHeader.length];
            reader.readFully(headerBytes);
            var origHeader = ArrayListHeader.fromBytes(headerBytes);

            // append
            var appendResult = db.readArrayListSlotAppend(origHeader, writeMode);
            // update the header before callbacks can freeze it
            var writer = db.core.writer();
            db.core.seek(nextArrayListStart);
            writer.write(appendResult.header().toBytes());
            // the appended moment of a top-level history becomes the transaction's
            // root slot, so it stays writable after a freeze. the history is the
            // copy of the root slot.
            if (db.transaction != null && slotPtr.position() != null && slotPtr.position().equals(db.transaction.scratchPosition)) {
                db.transaction.rootPosition = appendResult.slotPtr().position();
            }
            return db.readSlotPointer(writeMode, path, pathI + 1, appendResult.slotPtr());
        }
    }

    public static record ArrayListSlice(long size) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.ARRAY_LIST) throw new UnexpectedTagException();

            var reader = db.core.reader();
            slotPtr = db.copyCollectionIfFrozen(slotPtr, new ArrayListInit());
            var nextArrayListStart = slotPtr.slot().value();

            // read header
            db.core.seek(nextArrayListStart);
            var headerBytes = new byte[ArrayListHeader.length];
            reader.readFully(headerBytes);
            var origHeader = ArrayListHeader.fromBytes(headerBytes);

            // slice
            var sliceHeader = db.readArrayListSlice(origHeader, this.size());
            // update the header before callbacks can freeze it
            var writer = db.core.writer();
            db.core.seek(nextArrayListStart);
            writer.write(sliceHeader.toBytes());
            return db.readSlotPointer(writeMode, path, pathI + 1, slotPtr);
        }
    }

    public static record LinkedArrayListInit() implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.position() == null) throw new CursorNotWriteableException();
            long position = slotPtr.position();

            var writer = db.core.writer();

            switch (slotPtr.slot().tag()) {
                case NONE -> {
                    // create an empty tree: a single empty leaf plus a header
                    var rootPtr = db.writeBTreeNode(new BTreeNode(BTreeNodeKind.LEAF, 0));
                    var headerPtr = db.core.length();
                    db.core.seek(headerPtr);
                    writer.write(new BTreeHeader(rootPtr, 0).toBytes());
                    var nextSlotPtr = new SlotPointer(position, new Slot(headerPtr, Tag.LINKED_ARRAY_LIST));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                case LINKED_ARRAY_LIST -> {
                    var headerPtr = slotPtr.slot().value();
                    // copy the header into this transaction unless it was made in it,
                    // so past moments still pointing at the old header are unaffected.
                    // b-tree nodes are always appended, so only the header (updated in
                    // place by later operations in this tx) needs copying.
                    if (db.isOld(headerPtr)) {
                        var reader = db.core.reader();
                        db.core.seek(headerPtr);
                        var headerBytes = new byte[BTreeHeader.length];
                        reader.readFully(headerBytes);
                        headerPtr = db.core.length();
                        db.core.seek(headerPtr);
                        writer.write(headerBytes);
                    }
                    var nextSlotPtr = new SlotPointer(position, new Slot(headerPtr, Tag.LINKED_ARRAY_LIST));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                default -> throw new UnexpectedTagException();
            }
        }
    }

    public static record LinkedArrayListGet(long index) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case LINKED_ARRAY_LIST -> {}
                default -> throw new UnexpectedTagException();
            }

            var index = this.index();

            if (writeMode == WriteMode.READ_WRITE) slotPtr = db.copyCollectionIfFrozen(slotPtr, new LinkedArrayListInit());
            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytes = new byte[BTreeHeader.length];
            reader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);
            if (index >= header.size() || index < -header.size()) {
                throw new KeyNotFoundException();
            }
            var rank = index < 0 ? header.size() - Math.abs(index) : index;

            if (writeMode == WriteMode.READ_ONLY) {
                var finalSlotPtr = db.readBTreeSlot(header.rootPtr(), rank);
                return db.readSlotPointer(writeMode, path, pathI + 1, finalSlotPtr);
            } else {
                // path-copy down to the value slot so the write is persistent
                var writeSlot = db.btreeGetForWrite(header.rootPtr(), rank);
                // update the header before callbacks can freeze it
                if (writeSlot.nodePtr() != header.rootPtr()) {
                    var writer = db.core.writer();
                    db.core.seek(headerPtr);
                    writer.write(new BTreeHeader(writeSlot.nodePtr(), header.size()).toBytes());
                }
                return db.readSlotPointer(writeMode, path, pathI + 1, new SlotPointer(writeSlot.valuePosition(), writeSlot.slot()));
            }
        }
    }

    public static record LinkedArrayListAppend() implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            slotPtr = db.copyCollectionIfFrozen(slotPtr, new LinkedArrayListInit());
            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytes = new byte[BTreeHeader.length];
            reader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            var result = db.btreeInsert(header.rootPtr(), header.size());
            var newRootPtr = db.btreeGrowRoot(result);

            // update the header before filling in the value, so that a failure in
            // the rest of the path leaves the tree and header consistent
            var writer = db.core.writer();
            db.core.seek(headerPtr);
            writer.write(new BTreeHeader(newRootPtr, header.size() + 1).toBytes());

            // fill in the value via the rest of the path
            return db.readSlotPointer(writeMode, path, pathI + 1, new SlotPointer(result.valuePosition(), new Slot()));
        }
    }

    public static record LinkedArrayListSlice(long offset, long size) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            slotPtr = db.copyCollectionIfFrozen(slotPtr, new LinkedArrayListInit());
            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytes = new byte[BTreeHeader.length];
            reader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            // bounds-checked without overflow (offset + size could wrap)
            if (this.offset() > header.size() || this.size() > header.size() - this.offset()) {
                throw new KeyNotFoundException();
            }

            // slice = drop [0, offset) then keep [0, size) of what's left
            var afterOffset = db.btreeSplit(header.rootPtr(), this.offset());
            var sliced = db.btreeSplit(afterOffset.right(), this.size());
            var newRootPtr = sliced.left();

            // update the header before recursing into the rest of the path, so that
            // a failure there leaves the tree and header consistent
            var writer = db.core.writer();
            db.core.seek(headerPtr);
            writer.write(new BTreeHeader(newRootPtr, this.size()).toBytes());

            return db.readSlotPointer(writeMode, path, pathI + 1, slotPtr);
        }
    }

    public static record LinkedArrayListConcat(Slot list) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            if (this.list().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            slotPtr = db.copyCollectionIfFrozen(slotPtr, new LinkedArrayListInit());
            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytesA = new byte[BTreeHeader.length];
            reader.readFully(headerBytesA);
            var headerA = BTreeHeader.fromBytes(headerBytesA);
            db.core.seek(this.list().value());
            var headerBytesB = new byte[BTreeHeader.length];
            reader.readFully(headerBytesB);
            var headerB = BTreeHeader.fromBytes(headerBytesB);

            // the join result shares subtrees with both operands (and the second
            // operand stays live), so freeze everything created so far: later in-place
            // mutations will then copy those nodes instead of overwriting a node that
            // is still referenced elsewhere.
            db.txStart = db.core.length();
            var newRootPtr = db.btreeJoin(headerA.rootPtr(), headerB.rootPtr());

            // update the header before recursing into the rest of the path, so that
            // a failure there leaves the tree and header consistent
            var writer = db.core.writer();
            db.core.seek(headerPtr);
            writer.write(new BTreeHeader(newRootPtr, headerA.size() + headerB.size()).toBytes());

            return db.readSlotPointer(writeMode, path, pathI + 1, slotPtr);
        }
    }

    public static record LinkedArrayListInsert(long index) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            slotPtr = db.copyCollectionIfFrozen(slotPtr, new LinkedArrayListInit());
            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytes = new byte[BTreeHeader.length];
            reader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            var index = this.index();
            if (index >= header.size() || index < -header.size()) {
                throw new KeyNotFoundException();
            }
            var rank = index < 0 ? header.size() - Math.abs(index) : index;

            var result = db.btreeInsert(header.rootPtr(), rank);
            var newRootPtr = db.btreeGrowRoot(result);

            // update the header before filling in the value, so that a failure in
            // the rest of the path leaves the tree and header consistent
            var writer = db.core.writer();
            db.core.seek(headerPtr);
            writer.write(new BTreeHeader(newRootPtr, header.size() + 1).toBytes());

            return db.readSlotPointer(writeMode, path, pathI + 1, new SlotPointer(result.valuePosition(), new Slot()));
        }
    }

    public static record LinkedArrayListRemove(long index) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (slotPtr.slot().tag() != Tag.LINKED_ARRAY_LIST) throw new UnexpectedTagException();

            slotPtr = db.copyCollectionIfFrozen(slotPtr, new LinkedArrayListInit());
            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytes = new byte[BTreeHeader.length];
            reader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            var index = this.index();
            if (index >= header.size() || index < -header.size()) {
                throw new KeyNotFoundException();
            }
            var rank = index < 0 ? header.size() - Math.abs(index) : index;

            // remove = join the parts before and after the removed element
            var before = db.btreeSplit(header.rootPtr(), rank);
            var after = db.btreeSplit(before.right(), 1);
            var newRootPtr = db.btreeJoin(before.left(), after.right());

            // update the header before recursing into the rest of the path, so that
            // a failure there leaves the tree and header consistent
            var writer = db.core.writer();
            db.core.seek(headerPtr);
            writer.write(new BTreeHeader(newRootPtr, header.size() - 1).toBytes());

            return db.readSlotPointer(writeMode, path, pathI + 1, slotPtr);
        }
    }

    public static record HashMapInit(boolean counted, boolean set) implements PathPart {
        public HashMapInit() {
            this(false, false);
        }

        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            Tag tag = this.counted() ?
                        (this.set() ? Tag.COUNTED_HASH_SET : Tag.COUNTED_HASH_MAP) :
                        (this.set() ? Tag.HASH_SET : Tag.HASH_MAP);

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
                    if (db.isOld(mapStart)) {
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
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            boolean counted = false;
            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case HASH_MAP, HASH_SET -> {}
                case COUNTED_HASH_MAP, COUNTED_HASH_SET -> counted = true;
                default -> throw new UnexpectedTagException();
            }

            if (writeMode == WriteMode.READ_WRITE) slotPtr = db.copyCollectionIfFrozen(slotPtr, new HashMapInit(counted, slotPtr.slot().tag() == Tag.HASH_SET || slotPtr.slot().tag() == Tag.COUNTED_HASH_SET));
            long indexPos = counted ? slotPtr.slot().value() + 8 : slotPtr.slot().value();

            var res = db.readMapSlot(indexPos, db.checkHash(this.target()), (byte)0, writeMode, this.target());

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
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            boolean counted = false;
            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case HASH_MAP, HASH_SET -> {}
                case COUNTED_HASH_MAP, COUNTED_HASH_SET -> counted = true;
                default -> throw new UnexpectedTagException();
            }

            slotPtr = db.copyCollectionIfFrozen(slotPtr, new HashMapInit(counted, slotPtr.slot().tag() == Tag.HASH_SET || slotPtr.slot().tag() == Tag.COUNTED_HASH_SET));
            long indexPos = counted ? slotPtr.slot().value() + 8 : slotPtr.slot().value();

            boolean keyFound = true;
            try {
                db.removeMapSlot(indexPos, db.checkHash(this.hash()), (byte)0);
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

    public static record SortedMapInit(boolean set) implements PathPart {
        public SortedMapInit() {
            this(false);
        }

        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();
            if (slotPtr.position() == null) throw new CursorNotWriteableException();
            long position = slotPtr.position();
            Tag tag = this.set() ? Tag.SORTED_SET : Tag.SORTED_MAP;
            var writer = db.core.writer();
            switch (slotPtr.slot().tag()) {
                case NONE -> {
                    var rootPtr = db.writeSortedNode(new SortedNode(BTreeNodeKind.LEAF, 0));
                    var headerPtr = db.core.length();
                    db.core.seek(headerPtr);
                    writer.write(new BTreeHeader(rootPtr, 0).toBytes());
                    var nextSlotPtr = new SlotPointer(position, new Slot(headerPtr, tag));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                case SORTED_MAP, SORTED_SET -> {
                    if (slotPtr.slot().tag() != tag) throw new UnexpectedTagException();
                    var headerPtr = slotPtr.slot().value();
                    // copy the header into this transaction unless it was made in it
                    if (db.isOld(headerPtr)) {
                        var reader = db.core.reader();
                        db.core.seek(headerPtr);
                        var headerBytes = new byte[BTreeHeader.length];
                        reader.readFully(headerBytes);
                        headerPtr = db.core.length();
                        db.core.seek(headerPtr);
                        writer.write(headerBytes);
                    }
                    var nextSlotPtr = new SlotPointer(position, new Slot(headerPtr, tag));
                    db.core.seek(position);
                    writer.write(nextSlotPtr.slot().toBytes());
                    return db.readSlotPointer(writeMode, path, pathI + 1, nextSlotPtr);
                }
                default -> throw new UnexpectedTagException();
            }
        }
    }

    public static record SortedMapGet(SortedMapGetTarget target) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case SORTED_MAP, SORTED_SET -> {}
                default -> throw new UnexpectedTagException();
            }

            byte[] key;
            if (this.target() instanceof SortedMapGetKVPair t) key = t.key();
            else if (this.target() instanceof SortedMapGetKey t) key = t.key();
            else if (this.target() instanceof SortedMapGetValue t) key = t.key();
            else throw new IllegalArgumentException();

            if (writeMode == WriteMode.READ_WRITE) slotPtr = db.copyCollectionIfFrozen(slotPtr, new SortedMapInit(slotPtr.slot().tag() == Tag.SORTED_SET));
            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytes = new byte[BTreeHeader.length];
            reader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            if (writeMode == WriteMode.READ_ONLY) {
                var found = db.sortedGet(header.rootPtr(), key);
                if (found == null) throw new KeyNotFoundException();
                var targetSlot = db.sortedTargetSlot(found.slot(), this.target());
                return db.readSlotPointer(writeMode, path, pathI + 1, targetSlot);
            } else {
                var result = db.sortedPut(header.rootPtr(), key);
                var newRootPtr = db.sortedGrowRoot(result);

                // update the header before filling in the value, so that a failure in
                // the rest of the path leaves the tree and header consistent (the entry
                // exists with an empty value) rather than inserted-but-uncounted
                var writer = db.core.writer();
                db.core.seek(headerPtr);
                writer.write(new BTreeHeader(newRootPtr, header.size() + (result.added() ? 1 : 0)).toBytes());

                var targetSlot = db.sortedTargetSlot(result.kvSlot(), this.target());
                return db.readSlotPointer(writeMode, path, pathI + 1, targetSlot);
            }
        }
    }

    public static record SortedMapGetIndex(long index) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_WRITE) throw new WriteNotAllowedException();

            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case SORTED_MAP, SORTED_SET -> {}
                default -> throw new UnexpectedTagException();
            }

            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytes = new byte[BTreeHeader.length];
            reader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            var index = this.index();
            if (index >= header.size() || index < -header.size()) {
                throw new KeyNotFoundException();
            }
            var rank = index < 0 ? header.size() - Math.abs(index) : index;

            var found = db.sortedGetByIndex(header.rootPtr(), rank);
            // return the entry so the caller can read key and value
            var targetSlot = new SlotPointer(found.position(), found.slot());
            return db.readSlotPointer(writeMode, path, pathI + 1, targetSlot);
        }
    }

    public static record SortedMapRemove(byte[] key) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            switch (slotPtr.slot().tag()) {
                case NONE -> throw new KeyNotFoundException();
                case SORTED_MAP, SORTED_SET -> {}
                default -> throw new UnexpectedTagException();
            }

            slotPtr = db.copyCollectionIfFrozen(slotPtr, new SortedMapInit(slotPtr.slot().tag() == Tag.SORTED_SET));
            var headerPtr = slotPtr.slot().value();
            var reader = db.core.reader();
            db.core.seek(headerPtr);
            var headerBytes = new byte[BTreeHeader.length];
            reader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            var result = db.sortedRemove(header.rootPtr(), this.key());
            if (!result.found()) throw new KeyNotFoundException();

            var writer = db.core.writer();
            db.core.seek(headerPtr);
            writer.write(new BTreeHeader(result.nodePtr(), header.size() - 1).toBytes());

            return slotPtr;
        }
    }

    public static record WriteData(WriteableData data) implements PathPart {
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
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
        public SlotPointer readSlotPointer(Database db, WriteMode writeMode, PathPart[] path, int pathI, SlotPointer slotPtr) throws Exception {
            if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

            if (pathI != path.length - 1) throw new PathPartMustBeAtEndException();

            var nextCursor = new WriteCursor(slotPtr, db);
            this.function().run(nextCursor);
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

    public static sealed interface SortedMapGetTarget permits SortedMapGetKVPair, SortedMapGetKey, SortedMapGetValue {}
    public static record SortedMapGetKVPair(byte[] key) implements SortedMapGetTarget {}
    public static record SortedMapGetKey(byte[] key) implements SortedMapGetTarget {}
    public static record SortedMapGetValue(byte[] key) implements SortedMapGetTarget {}

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
    // exceptions

    public static class DatabaseException extends RuntimeException {}
    public static class NotImplementedException extends DatabaseException {}
    public static class UnreachableException extends DatabaseException {}
    public static class InvalidDatabaseException extends DatabaseException {}
    public static class TruncatedDatabaseException extends DatabaseException {}
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
    public static class ExpectedUnsignedLongException extends DatabaseException {}
    public static class NoAvailableSlotsException extends DatabaseException {}
    public static class MustSetNewSlotsToFullException extends DatabaseException {}
    public static class EmptySlotException extends DatabaseException {}
    public static class ExpectedRootNodeException extends DatabaseException {}
    public static class InvalidFormatTagSizeException extends DatabaseException {}
    public static class UnexpectedWriterPositionException extends DatabaseException {}
    public static class MaxShiftExceededException extends DatabaseException {}
    public static class InvalidBTreeNodeException extends DatabaseException {}
    public static class InvalidBTreeNodeKindException extends DatabaseException {}
    public static class InvalidRangeException extends DatabaseException {}
    public static class TargetNotEmptyException extends DatabaseException {}

    // hash_map

    public static record HashMapGetResult(SlotPointer slotPtr, boolean isEmpty) {}

    private HashMapGetResult readMapSlot(long indexPos, byte[] keyHash, byte keyOffset, WriteMode writeMode, HashMapGetTarget target) throws IOException {
        if (keyOffset > (this.header.hashSize() * 8) / BIT_COUNT) {
            throw new KeyOffsetExceededException();
        }

        var reader = this.core.reader();

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
                        var writer = this.core.writer();
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
                if (writeMode == WriteMode.READ_WRITE) {
                    nextPtr = copyIndexBlockIfOld(ptr);
                    if (nextPtr != ptr) {
                        // make slot point to the copied block
                        this.core.seek(slotPos);
                        this.core.writer().write(new Slot(nextPtr, Tag.INDEX).toBytes());
                    }
                }
                return readMapSlot(nextPtr, keyHash, (byte) (keyOffset + 1), writeMode, target);
            }
            case KV_PAIR -> {
                this.core.seek(ptr);
                var kvPairBytes = new byte[KeyValuePair.length(this.header.hashSize())];
                reader.readFully(kvPairBytes);
                var kvPair = KeyValuePair.fromBytes(kvPairBytes, this.header.hashSize());

                if (Arrays.equals(kvPair.hash(), keyHash)) {
                    if (writeMode == WriteMode.READ_WRITE) {
                        if (isOld(ptr)) {
                            var writer = this.core.writer();
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
                            var writer = this.core.writer();
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
                            var res = readMapSlot(nextIndexPos, keyHash, (byte) (keyOffset + 1), writeMode, target);
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

    private Slot removeMapSlot(long indexPos, byte[] keyHash, byte keyOffset) throws IOException {
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
            case INDEX -> removeMapSlot(slot.value(), keyHash, (byte) (keyOffset + 1));
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
        // so we must keep this index block. update the slot in it,
        // copying the block first unless it belongs to this transaction.
        var nextIndexPos = copyIndexBlockIfOld(indexPos);
        this.core.seek(nextIndexPos + (Slot.length * i));
        writer.write(nextSlot.toBytes());
        return new Slot(nextIndexPos, Tag.INDEX);
    }

    // b-tree

    BTreeNode readBTreeNode(long ptr) throws IOException {
        this.core.seek(ptr);
        var reader = this.core.reader();
        var headerBytes = new byte[BTREE_NODE_HEADER_SIZE];
        reader.readFully(headerBytes);
        var kindInt = headerBytes[0] & 0xFF;
        if (kindInt >= BTreeNodeKind.values().length) throw new InvalidBTreeNodeKindException();
        var kind = BTreeNodeKind.values()[kindInt];
        var num = headerBytes[1] & 0xFF;
        if (num > BTREE_SLOT_COUNT) throw new InvalidBTreeNodeException();
        var node = new BTreeNode(kind, num);
        switch (kind) {
            case LEAF -> {
                var body = new byte[Slot.length * BTREE_SLOT_COUNT];
                reader.readFully(body);
                var buffer = ByteBuffer.wrap(body);
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                    var slotBytes = new byte[Slot.length];
                    buffer.get(slotBytes);
                    node.values[i] = Slot.fromBytes(slotBytes);
                }
            }
            case BRANCH -> {
                var body = new byte[(Slot.length + 8) * BTREE_SLOT_COUNT];
                reader.readFully(body);
                var buffer = ByteBuffer.wrap(body);
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                    var slotBytes = new byte[Slot.length];
                    buffer.get(slotBytes);
                    node.children[i] = Slot.fromBytes(slotBytes);
                }
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                    node.counts[i] = buffer.getLong();
                }
            }
        }
        return node;
    }

    // always writes the node as a block at ptr. b-tree mutations are persistent:
    // every node on the path from the root is rewritten, while untouched subtrees
    // are shared by pointer.
    private void writeBTreeNodeAt(BTreeNode node, long ptr) throws IOException {
        this.core.seek(ptr);
        var writer = this.core.writer();
        int bodySize = node.kind == BTreeNodeKind.LEAF
            ? BTREE_LEAF_BLOCK_SIZE
            : BTREE_BRANCH_BLOCK_SIZE;
        var buffer = ByteBuffer.allocate(bodySize);
        buffer.put((byte) node.kind.ordinal());
        buffer.put((byte) node.num);
        switch (node.kind) {
            case LEAF -> {
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) buffer.put(node.values[i].toBytes());
            }
            case BRANCH -> {
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) buffer.put(node.children[i].toBytes());
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) buffer.putLong(node.counts[i]);
            }
        }
        writer.write(buffer.array());
    }

    // appends the node as a fresh block and returns its position
    private long writeBTreeNode(BTreeNode node) throws IOException {
        var ptr = this.core.length();
        writeBTreeNodeAt(node, ptr);
        return ptr;
    }

    // a node is safe to mutate in place when it was created in the current
    // transaction (offset >= txStart), since no committed moment and no pre-concat
    // sharing can reference it. concat advances txStart (an implicit freeze)
    // precisely so its shared subtrees fall below it here.
    private boolean btreeReusable(long ptr) throws IOException {
        // nothing can be reused outside a transaction
        try {
            return !isOld(ptr);
        } catch (ExpectedTxStartException e) {
            return false;
        }
    }

    // write a new version of a node, reusing oldPtr's position in place if that node
    // belongs to this transaction, otherwise appending a copy
    private long btreeWriteNode(BTreeNode node, long oldPtr) throws IOException {
        if (btreeReusable(oldPtr)) {
            writeBTreeNodeAt(node, oldPtr);
            return oldPtr;
        }
        return writeBTreeNode(node);
    }

    private long btreeNewRoot() throws IOException {
        return writeBTreeNode(new BTreeNode(BTreeNodeKind.LEAF, 0));
    }

    // descend to the value slot at the given rank (0-based), returning a pointer
    // to it (its file position and current slot).
    private SlotPointer readBTreeSlot(long rootPtr, long rank) throws IOException {
        var nodePtr = rootPtr;
        var rem = rank;
        while (true) {
            var node = readBTreeNode(nodePtr);
            switch (node.kind) {
                case LEAF -> {
                    var position = nodePtr + BTREE_NODE_HEADER_SIZE + rem * Slot.length;
                    return new SlotPointer(position, node.values[(int) rem]);
                }
                case BRANCH -> {
                    int i = 0;
                    while (i + 1 < node.num && rem >= node.counts[i]) {
                        rem -= node.counts[i];
                        i++;
                    }
                    nodePtr = node.children[i].value();
                }
            }
        }
    }

    // insert a placeholder slot at `rank` within the subtree at nodePtr, writing new
    // nodes along the path. the caller fills in the value at the returned valuePosition.
    private BTreeInsertResult btreeInsert(long nodePtr, long rank) throws IOException {
        var node = readBTreeNode(nodePtr);
        switch (node.kind) {
            case LEAF -> {
                // build the entries with a placeholder spliced in at `rank`. the
                // placeholder is a NONE slot marked full so that, if the caller never
                // writes a value (e.g. appendCursor), iteration still counts it as an
                // element rather than skipping it as padding.
                var vals = new Slot[BTREE_SLOT_COUNT + 1];
                int r = (int) rank;
                System.arraycopy(node.values, 0, vals, 0, r);
                vals[r] = new Slot(0, Tag.NONE, true);
                System.arraycopy(node.values, r, vals, r + 1, node.num - r);
                int total = node.num + 1;

                if (total <= BTREE_SLOT_COUNT) {
                    var leaf = new BTreeNode(BTreeNodeKind.LEAF, total);
                    System.arraycopy(vals, 0, leaf.values, 0, total);
                    var ptr = btreeWriteNode(leaf, nodePtr);
                    return new BTreeInsertResult(ptr, total, ptr + BTREE_NODE_HEADER_SIZE + (long) r * Slot.length, null);
                }

                // overflow: split into two leaves (reuse this node for the left half)
                int leftN = BTREE_SPLIT_COUNT;
                int rightN = total - leftN;
                var left = new BTreeNode(BTreeNodeKind.LEAF, leftN);
                System.arraycopy(vals, 0, left.values, 0, leftN);
                var right = new BTreeNode(BTreeNodeKind.LEAF, rightN);
                System.arraycopy(vals, leftN, right.values, 0, rightN);
                var leftPtr = btreeWriteNode(left, nodePtr);
                var rightPtr = writeBTreeNode(right);
                long valuePosition = (r < leftN)
                    ? leftPtr + BTREE_NODE_HEADER_SIZE + (long) r * Slot.length
                    : rightPtr + BTREE_NODE_HEADER_SIZE + (long) (r - leftN) * Slot.length;
                return new BTreeInsertResult(leftPtr, leftN, valuePosition, new BTreeNodeRef(rightPtr, rightN));
            }
            case BRANCH -> {
                // pick the child that contains `rank`
                int i = 0;
                long rem = rank;
                while (i + 1 < node.num && rem > node.counts[i]) {
                    rem -= node.counts[i];
                    i++;
                }
                var child = btreeInsert(node.children[i].value(), rem);

                // rebuild this branch with the (possibly split) child
                var children = new Slot[BTREE_SLOT_COUNT + 1];
                var counts = new long[BTREE_SLOT_COUNT + 1];
                System.arraycopy(node.children, 0, children, 0, node.num);
                System.arraycopy(node.counts, 0, counts, 0, node.num);
                children[i] = new Slot(child.nodePtr(), Tag.INDEX);
                counts[i] = child.count();
                int total = node.num;
                if (child.split() != null) {
                    for (int j = node.num; j > i + 1; j--) {
                        children[j] = children[j - 1];
                        counts[j] = counts[j - 1];
                    }
                    children[i + 1] = new Slot(child.split().nodePtr(), Tag.INDEX);
                    counts[i + 1] = child.split().count();
                    total = node.num + 1;
                }

                if (total <= BTREE_SLOT_COUNT) {
                    var branch = new BTreeNode(BTreeNodeKind.BRANCH, total);
                    System.arraycopy(children, 0, branch.children, 0, total);
                    System.arraycopy(counts, 0, branch.counts, 0, total);
                    var ptr = btreeWriteNode(branch, nodePtr);
                    return new BTreeInsertResult(ptr, branch.subtreeCount(), child.valuePosition(), null);
                }

                // overflow: split into two branches (reuse this node for the left half)
                int leftN = BTREE_SPLIT_COUNT;
                int rightN = total - leftN;
                var left = new BTreeNode(BTreeNodeKind.BRANCH, leftN);
                System.arraycopy(children, 0, left.children, 0, leftN);
                System.arraycopy(counts, 0, left.counts, 0, leftN);
                var right = new BTreeNode(BTreeNodeKind.BRANCH, rightN);
                System.arraycopy(children, leftN, right.children, 0, rightN);
                System.arraycopy(counts, leftN, right.counts, 0, rightN);
                var leftPtr = btreeWriteNode(left, nodePtr);
                var rightPtr = writeBTreeNode(right);
                return new BTreeInsertResult(leftPtr, left.subtreeCount(), child.valuePosition(),
                    new BTreeNodeRef(rightPtr, right.subtreeCount()));
            }
        }
        throw new UnreachableException();
    }

    // turn an insert result into a root pointer, growing the tree a level if the old
    // root split (shares the root-building logic with btreeMakeRoot)
    private long btreeGrowRoot(BTreeInsertResult result) throws IOException {
        return btreeMakeRoot(new BTreeJoinResult(result.nodePtr(), result.count(), result.split()));
    }

    // descend to the value slot at `rank` for writing, copy-on-writing only the nodes
    // that belong to a past transaction. the element count is unchanged, so when the
    // whole path is already this-transaction nothing is rewritten and the caller
    // writes straight into the existing leaf.
    private BTreeWriteSlot btreeGetForWrite(long nodePtr, long rank) throws IOException {
        var node = readBTreeNode(nodePtr);
        switch (node.kind) {
            case LEAF -> {
                var newPtr = btreeReusable(nodePtr) ? nodePtr : writeBTreeNode(node);
                return new BTreeWriteSlot(newPtr, newPtr + BTREE_NODE_HEADER_SIZE + rank * Slot.length, node.values[(int) rank]);
            }
            case BRANCH -> {
                int i = 0;
                long rem = rank;
                while (i + 1 < node.num && rem >= node.counts[i]) {
                    rem -= node.counts[i];
                    i++;
                }
                var child = btreeGetForWrite(node.children[i].value(), rem);
                // if the child stayed put, this branch is unchanged too
                if (child.nodePtr() == node.children[i].value()) {
                    return new BTreeWriteSlot(nodePtr, child.valuePosition(), child.slot());
                }
                node.children[i] = new Slot(child.nodePtr(), Tag.INDEX);
                var newPtr = btreeWriteNode(node, nodePtr);
                return new BTreeWriteSlot(newPtr, child.valuePosition(), child.slot());
            }
        }
        throw new UnreachableException();
    }

    // join (concat): a true O(log n), structure-sharing concatenation of two trees
    // where every element of `a` precedes every element of `b`. unlike the rebuild
    // helpers above, untouched subtrees are shared by pointer, so concatenating a
    // list with itself stays cheap.

    // height of a tree = number of branch levels above the leaves
    private int btreeHeight(long rootPtr) throws IOException {
        var ptr = rootPtr;
        int height = 0;
        while (true) {
            var node = readBTreeNode(ptr);
            if (node.kind == BTreeNodeKind.LEAF) return height;
            height++;
            ptr = node.children[0].value();
        }
    }

    private long btreeMakeRoot(BTreeJoinResult result) throws IOException {
        if (result.split() != null) {
            var root = new BTreeNode(BTreeNodeKind.BRANCH, 2);
            root.children[0] = new Slot(result.nodePtr(), Tag.INDEX);
            root.children[1] = new Slot(result.split().nodePtr(), Tag.INDEX);
            root.counts[0] = result.count();
            root.counts[1] = result.split().count();
            return writeBTreeNode(root);
        }
        return result.nodePtr();
    }

    // write `vals` as one leaf, or split into two balanced leaves if it exceeds the
    // node capacity
    private BTreeJoinResult btreeAssembleLeaf(Slot[] vals, int total) throws IOException {
        if (total <= BTREE_SLOT_COUNT) {
            var leaf = new BTreeNode(BTreeNodeKind.LEAF, total);
            System.arraycopy(vals, 0, leaf.values, 0, total);
            return new BTreeJoinResult(writeBTreeNode(leaf), total, null);
        }
        int leftN = total / 2;
        var left = new BTreeNode(BTreeNodeKind.LEAF, leftN);
        System.arraycopy(vals, 0, left.values, 0, leftN);
        var right = new BTreeNode(BTreeNodeKind.LEAF, total - leftN);
        System.arraycopy(vals, leftN, right.values, 0, total - leftN);
        return new BTreeJoinResult(writeBTreeNode(left), leftN, new BTreeNodeRef(writeBTreeNode(right), total - leftN));
    }

    // write `children`/`counts` as one branch, or split into two balanced branches
    private BTreeJoinResult btreeAssembleBranch(Slot[] children, long[] counts, int total) throws IOException {
        if (total <= BTREE_SLOT_COUNT) {
            var branch = new BTreeNode(BTreeNodeKind.BRANCH, total);
            System.arraycopy(children, 0, branch.children, 0, total);
            System.arraycopy(counts, 0, branch.counts, 0, total);
            return new BTreeJoinResult(writeBTreeNode(branch), branch.subtreeCount(), null);
        }
        int leftN = total / 2;
        var left = new BTreeNode(BTreeNodeKind.BRANCH, leftN);
        System.arraycopy(children, 0, left.children, 0, leftN);
        System.arraycopy(counts, 0, left.counts, 0, leftN);
        var right = new BTreeNode(BTreeNodeKind.BRANCH, total - leftN);
        System.arraycopy(children, leftN, right.children, 0, total - leftN);
        System.arraycopy(counts, leftN, right.counts, 0, total - leftN);
        return new BTreeJoinResult(writeBTreeNode(left), left.subtreeCount(),
            new BTreeNodeRef(writeBTreeNode(right), right.subtreeCount()));
    }

    // merge two nodes of equal height (a precedes b) into one or two nodes
    private BTreeJoinResult btreeMergeNodes(BTreeNode a, BTreeNode b) throws IOException {
        switch (a.kind) {
            case LEAF -> {
                var vals = new Slot[2 * BTREE_SLOT_COUNT];
                System.arraycopy(a.values, 0, vals, 0, a.num);
                System.arraycopy(b.values, 0, vals, a.num, b.num);
                return btreeAssembleLeaf(vals, a.num + b.num);
            }
            case BRANCH -> {
                var children = new Slot[2 * BTREE_SLOT_COUNT];
                var counts = new long[2 * BTREE_SLOT_COUNT];
                System.arraycopy(a.children, 0, children, 0, a.num);
                System.arraycopy(a.counts, 0, counts, 0, a.num);
                System.arraycopy(b.children, 0, children, a.num, b.num);
                System.arraycopy(b.counts, 0, counts, a.num, b.num);
                return btreeAssembleBranch(children, counts, a.num + b.num);
            }
        }
        throw new UnreachableException();
    }

    // join b (shorter) into the rightmost spine of a (taller), at height hb
    private BTreeJoinResult btreeJoinRight(long aPtr, int ha, long bPtr, int hb) throws IOException {
        var a = readBTreeNode(aPtr);
        int last = a.num - 1;
        var sub = (ha - 1 == hb)
            ? btreeMergeNodes(readBTreeNode(a.children[last].value()), readBTreeNode(bPtr))
            : btreeJoinRight(a.children[last].value(), ha - 1, bPtr, hb);

        var children = new Slot[BTREE_SLOT_COUNT + 1];
        var counts = new long[BTREE_SLOT_COUNT + 1];
        System.arraycopy(a.children, 0, children, 0, a.num);
        System.arraycopy(a.counts, 0, counts, 0, a.num);
        children[last] = new Slot(sub.nodePtr(), Tag.INDEX);
        counts[last] = sub.count();
        int total = a.num;
        if (sub.split() != null) {
            children[total] = new Slot(sub.split().nodePtr(), Tag.INDEX);
            counts[total] = sub.split().count();
            total += 1;
        }
        return btreeAssembleBranch(children, counts, total);
    }

    // join a (shorter) into the leftmost spine of b (taller), at height ha
    private BTreeJoinResult btreeJoinLeft(long aPtr, int ha, long bPtr, int hb) throws IOException {
        var b = readBTreeNode(bPtr);
        var sub = (hb - 1 == ha)
            ? btreeMergeNodes(readBTreeNode(aPtr), readBTreeNode(b.children[0].value()))
            : btreeJoinLeft(aPtr, ha, b.children[0].value(), hb - 1);

        var children = new Slot[BTREE_SLOT_COUNT + 1];
        var counts = new long[BTREE_SLOT_COUNT + 1];
        children[0] = new Slot(sub.nodePtr(), Tag.INDEX);
        counts[0] = sub.count();
        int head = 1;
        if (sub.split() != null) {
            children[1] = new Slot(sub.split().nodePtr(), Tag.INDEX);
            counts[1] = sub.split().count();
            head = 2;
        }
        System.arraycopy(b.children, 1, children, head, b.num - 1);
        System.arraycopy(b.counts, 1, counts, head, b.num - 1);
        return btreeAssembleBranch(children, counts, head + b.num - 1);
    }

    private long btreeJoin(long rootA, long rootB) throws IOException {
        int ha = btreeHeight(rootA);
        int hb = btreeHeight(rootB);
        BTreeJoinResult result;
        if (ha == hb) {
            result = btreeMergeNodes(readBTreeNode(rootA), readBTreeNode(rootB));
        } else if (ha > hb) {
            result = btreeJoinRight(rootA, ha, rootB, hb);
        } else {
            result = btreeJoinLeft(rootA, ha, rootB, hb);
        }
        return btreeMakeRoot(result);
    }

    // split (used by slice and remove): a true O(log n), structure-sharing split of a
    // tree into [0, rank) and [rank, size). partial nodes along the path are
    // reassembled with join, so the result trees stay balanced.

    // build a tree from a run of sibling children (already height-h-1 subtrees):
    // empty -> a new empty leaf, one -> that child unwrapped, many -> a branch
    private long btreeSubtree(Slot[] children, long[] counts, int start, int len) throws IOException {
        if (len == 0) return btreeNewRoot();
        if (len == 1) return children[start].value();
        // len <= BTREE_SLOT_COUNT here, so this never splits
        var subChildren = new Slot[len];
        var subCounts = new long[len];
        System.arraycopy(children, start, subChildren, 0, len);
        System.arraycopy(counts, start, subCounts, 0, len);
        return btreeAssembleBranch(subChildren, subCounts, len).nodePtr();
    }

    private BTreeSplitResult btreeSplit(long rootPtr, long rank) throws IOException {
        var node = readBTreeNode(rootPtr);
        switch (node.kind) {
            case LEAF -> {
                int r = (int) rank;
                var left = new BTreeNode(BTreeNodeKind.LEAF, r);
                System.arraycopy(node.values, 0, left.values, 0, r);
                var right = new BTreeNode(BTreeNodeKind.LEAF, node.num - r);
                System.arraycopy(node.values, r, right.values, 0, node.num - r);
                return new BTreeSplitResult(writeBTreeNode(left), writeBTreeNode(right));
            }
            case BRANCH -> {
                int i = 0;
                long rem = rank;
                while (i + 1 < node.num && rem > node.counts[i]) {
                    rem -= node.counts[i];
                    i++;
                }
                var child = btreeSplit(node.children[i].value(), rem);
                var leftSub = btreeSubtree(node.children, node.counts, 0, i);
                var rightSub = btreeSubtree(node.children, node.counts, i + 1, node.num - (i + 1));
                return new BTreeSplitResult(btreeJoin(leftSub, child.left()), btreeJoin(child.right(), rightSub));
            }
        }
        throw new UnreachableException();
    }

    // array_list

    public static record ArrayListAppendResult(ArrayListHeader header, SlotPointer slotPtr) {}

    private ArrayListAppendResult readArrayListSlotAppend(ArrayListHeader header, WriteMode writeMode) throws IOException {
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

        var slotPtr = readArrayListSlot(indexPos, key, nextShift, writeMode);
        // clear values left by a rollback or slice
        slotPtr = slotPtr.withSlot(new Slot());
        this.core.seek(slotPtr.position());
        writer.write(slotPtr.slot().toBytes());
        return new ArrayListAppendResult(new ArrayListHeader(indexPos, header.size() + 1), slotPtr);
    }

    private SlotPointer readArrayListSlot(long indexPos, long key, byte shift, WriteMode writeMode) throws IOException {
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
                        this.core.seek(slotPos);
                        writer.write(new Slot(nextIndexPos, Tag.INDEX).toBytes());
                        return readArrayListSlot(nextIndexPos, key, (byte)(shift - 1), writeMode);
                    }
                    default -> throw new UnreachableException();
                }
            }
            case INDEX -> {
                var nextPtr = ptr;
                if (writeMode == WriteMode.READ_WRITE) {
                    nextPtr = copyIndexBlockIfOld(ptr);
                    if (nextPtr != ptr) {
                        // make slot point to the copied block
                        var writer = this.core.writer();
                        this.core.seek(slotPos);
                        writer.write(new Slot(nextPtr, Tag.INDEX).toBytes());
                    }
                }
                return readArrayListSlot(nextPtr, key, (byte)(shift - 1), writeMode);
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
            // the new root may still belong to a past moment. unlike child
            // nodes, root nodes are written directly, so copy it now.
            indexPos = copyIndexBlockIfOld(indexPos);
            return new ArrayListHeader(indexPos, size);
        }
    }

    // sorted_map / sorted_set

    SortedNode readSortedNode(long ptr) throws IOException {
        this.core.seek(ptr);
        var reader = this.core.reader();
        var headerBytes = new byte[BTREE_NODE_HEADER_SIZE];
        reader.readFully(headerBytes);
        var kindInt = headerBytes[0] & 0xFF;
        if (kindInt >= BTreeNodeKind.values().length) throw new InvalidBTreeNodeKindException();
        var kind = BTreeNodeKind.values()[kindInt];
        var num = headerBytes[1] & 0xFF;
        if (num > BTREE_SLOT_COUNT) throw new InvalidBTreeNodeException();
        var node = new SortedNode(kind, num);
        switch (kind) {
            case LEAF -> {
                var body = new byte[Slot.length * BTREE_SLOT_COUNT];
                reader.readFully(body);
                var buffer = ByteBuffer.wrap(body);
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                    var slotBytes = new byte[Slot.length];
                    buffer.get(slotBytes);
                    node.entries[i] = Slot.fromBytes(slotBytes);
                }
            }
            case BRANCH -> {
                var body = new byte[(Slot.length * 2 + 8) * BTREE_SLOT_COUNT];
                reader.readFully(body);
                var buffer = ByteBuffer.wrap(body);
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                    var slotBytes = new byte[Slot.length];
                    buffer.get(slotBytes);
                    node.children[i] = Slot.fromBytes(slotBytes);
                }
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                    var slotBytes = new byte[Slot.length];
                    buffer.get(slotBytes);
                    node.separators[i] = Slot.fromBytes(slotBytes);
                }
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                    node.counts[i] = buffer.getLong();
                }
            }
        }
        return node;
    }

    private void writeSortedNodeAt(SortedNode node, long ptr) throws IOException {
        this.core.seek(ptr);
        var writer = this.core.writer();
        int bodySize = node.kind == BTreeNodeKind.LEAF
            ? SORTED_LEAF_BLOCK_SIZE
            : SORTED_BRANCH_BLOCK_SIZE;
        var buffer = ByteBuffer.allocate(bodySize);
        buffer.put((byte) node.kind.ordinal());
        buffer.put((byte) node.num);
        switch (node.kind) {
            case LEAF -> {
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) buffer.put(node.entries[i].toBytes());
            }
            case BRANCH -> {
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) buffer.put(node.children[i].toBytes());
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) buffer.put(node.separators[i].toBytes());
                for (int i = 0; i < BTREE_SLOT_COUNT; i++) buffer.putLong(node.counts[i]);
            }
        }
        writer.write(buffer.array());
    }

    private long writeSortedNode(SortedNode node) throws IOException {
        var ptr = this.core.length();
        writeSortedNodeAt(node, ptr);
        return ptr;
    }

    // reuse oldPtr's position in place when it belongs to this transaction
    // (mirrors btreeWriteNode / the txStart path-copying model)
    private long sortedWriteNode(SortedNode node, long oldPtr) throws IOException {
        if (btreeReusable(oldPtr)) {
            writeSortedNodeAt(node, oldPtr);
            return oldPtr;
        }
        return writeSortedNode(node);
    }

    // decodes a .kv_pair or .sorted_kv_pair entry. a .sorted_kv_pair has no hash, so it is 0.
    private static KeyValuePair decodeKvPair(DataInput reader, Tag tag, int hashSize) throws IOException {
        switch (tag) {
            case KV_PAIR -> {
                var bytes = new byte[KeyValuePair.length(hashSize)];
                reader.readFully(bytes);
                return KeyValuePair.fromBytes(bytes, hashSize);
            }
            case SORTED_KV_PAIR -> {
                var bytes = new byte[SortedKeyValuePair.length];
                reader.readFully(bytes);
                var pair = SortedKeyValuePair.fromBytes(bytes);
                return new KeyValuePair(pair.valueSlot(), pair.keySlot(), new byte[hashSize]);
            }
            default -> throw new UnexpectedTagException();
        }
    }

    // encodes a .kv_pair or .sorted_kv_pair entry. a .sorted_kv_pair leaves out the hash.
    private static void encodeKvPair(DataOutput writer, Tag tag, KeyValuePair kvPair) throws IOException {
        switch (tag) {
            case KV_PAIR -> writer.write(kvPair.toBytes());
            case SORTED_KV_PAIR -> writer.write(new SortedKeyValuePair(kvPair.valueSlot(), kvPair.keySlot()).toBytes());
            default -> throw new UnreachableException();
        }
    }

    KeyValuePair readKvPair(Slot kvSlot) throws IOException {
        this.core.seek(kvSlot.value());
        return decodeKvPair(this.core.reader(), kvSlot.tag(), this.header.hashSize());
    }

    // writes a .kv_pair or .sorted_kv_pair entry at the end and returns its slot
    private Slot writeKvPair(Tag tag, KeyValuePair kvPair) throws IOException {
        var pos = this.core.length();
        this.core.seek(pos);
        encodeKvPair(this.core.writer(), tag, kvPair);
        return new Slot(pos, tag);
    }

    // lexicographic comparison of the byte key stored at keySlot (a bytes or
    // short_bytes slot) against the in-memory target. returns <0, 0, or >0. streams
    // external bytes so keys of any length work without allocation.
    int compareKey(Slot keySlot, byte[] target) throws IOException {
        switch (keySlot.tag()) {
            case SHORT_BYTES -> {
                var buf = ByteBuffer.allocate(8);
                buf.putLong(keySlot.value());
                var bytes = buf.array();
                int total = keySlot.full() ? 6 : 8;
                int len = total;
                for (int i = 0; i < total; i++) {
                    if (bytes[i] == 0) { len = i; break; }
                }
                return Arrays.compareUnsigned(Arrays.copyOf(bytes, len), target);
            }
            case BYTES -> {
                var reader = this.core.reader();
                this.core.seek(keySlot.value());
                long len = reader.readLong();
                long i = 0;
                var chunk = new byte[256];
                while (i < len) {
                    int n = (int) Math.min(chunk.length, len - i);
                    reader.readFully(chunk, 0, n);
                    for (int j = 0; j < n; j++) {
                        long ti = i + j;
                        if (ti >= target.length) return 1; // stored has more, equal so far
                        int b = chunk[j] & 0xFF;
                        int t = target[(int) ti] & 0xFF;
                        if (b < t) return -1;
                        if (b > t) return 1;
                    }
                    i += n;
                }
                return target.length > len ? -1 : 0;
            }
            default -> throw new UnexpectedTagException();
        }
    }

    // descend by key to the matching leaf entry, or null
    private SortedSlot sortedGet(long rootPtr, byte[] key) throws IOException {
        var nodePtr = rootPtr;
        while (true) {
            var node = readSortedNode(nodePtr);
            switch (node.kind) {
                case LEAF -> {
                    for (int i = 0; i < node.num; i++) {
                        var entry = node.entries[i];
                        var kv = readKvPair(entry);
                        int cmp = compareKey(kv.keySlot(), key);
                        if (cmp == 0) return new SortedSlot(entry, nodePtr + BTREE_NODE_HEADER_SIZE + (long) i * Slot.length);
                        if (cmp > 0) return null;
                    }
                    return null;
                }
                case BRANCH -> {
                    int i = 0;
                    while (i + 1 < node.num && compareKey(node.separators[i + 1], key) <= 0) i++;
                    nodePtr = node.children[i].value();
                }
            }
        }
    }

    // descend by rank to the leaf entry at the given 0-based index
    private SortedSlot sortedGetByIndex(long rootPtr, long rank) throws IOException {
        var nodePtr = rootPtr;
        var rem = rank;
        while (true) {
            var node = readSortedNode(nodePtr);
            switch (node.kind) {
                case LEAF -> {
                    int i = (int) rem;
                    return new SortedSlot(node.entries[i], nodePtr + BTREE_NODE_HEADER_SIZE + (long) i * Slot.length);
                }
                case BRANCH -> {
                    int i = 0;
                    while (i + 1 < node.num && rem >= node.counts[i]) { rem -= node.counts[i]; i++; }
                    nodePtr = node.children[i].value();
                }
            }
        }
    }

    // number of keys strictly less than key (the inverse of getByIndex)
    long sortedRank(long rootPtr, byte[] key) throws IOException {
        var nodePtr = rootPtr;
        long rank = 0;
        while (true) {
            var node = readSortedNode(nodePtr);
            switch (node.kind) {
                case LEAF -> {
                    for (int i = 0; i < node.num; i++) {
                        var kv = readKvPair(node.entries[i]);
                        if (compareKey(kv.keySlot(), key) < 0) rank += 1;
                        else break;
                    }
                    return rank;
                }
                case BRANCH -> {
                    int i = 0;
                    while (i + 1 < node.num && compareKey(node.separators[i + 1], key) <= 0) { rank += node.counts[i]; i++; }
                    nodePtr = node.children[i].value();
                }
            }
        }
    }

    // write a byte key as a short_bytes (inline, <=8 bytes, no interior zero) or
    // external bytes slot
    private Slot writeKey(byte[] key) throws IOException {
        boolean hasZero = false;
        for (byte b : key) {
            if (b == 0) { hasZero = true; break; }
        }
        if (key.length <= 8 && !hasZero) {
            var value = new byte[8];
            System.arraycopy(key, 0, value, 0, key.length);
            var buf = ByteBuffer.wrap(value);
            return new Slot(buf.getLong(), Tag.SHORT_BYTES);
        }
        var writer = this.core.writer();
        var pos = this.core.length();
        this.core.seek(pos);
        writer.writeLong(key.length);
        writer.write(key);
        return new Slot(pos, Tag.BYTES);
    }

    // materialize a new leaf entry: write the key bytes and a key-value pair with
    // an empty value (the caller fills it in). navigation is by key bytes, so the
    // entry has no hash.
    private SortedEntry sortedNewEntry(byte[] key) throws IOException {
        var keySlot = writeKey(key);
        var kvSlot = writeKvPair(Tag.SORTED_KV_PAIR, new KeyValuePair(new Slot(), keySlot, new byte[this.header.hashSize()]));
        return new SortedEntry(kvSlot, keySlot);
    }

    // insert key (or locate it for replacement) within the subtree at nodePtr,
    // path-copying nodes and maintaining separators + counts. the caller writes the
    // value of the returned entry.
    private SortedInsertResult sortedPut(long nodePtr, byte[] key) throws IOException {
        var node = readSortedNode(nodePtr);
        switch (node.kind) {
            case LEAF -> {
                // find the matching or insertion index
                int idx = node.num;
                boolean found = false;
                for (int i = 0; i < node.num; i++) {
                    var kv = readKvPair(node.entries[i]);
                    int cmp = compareKey(kv.keySlot(), key);
                    if (cmp == 0) { idx = i; found = true; break; }
                    if (cmp > 0) { idx = i; break; }
                }

                if (found) {
                    // replace: return a writable value slot, copy-on-writing the
                    // kv_pair if it belongs to a past moment
                    var leaf = node;
                    var kvSlot = node.entries[idx];
                    if (!btreeReusable(kvSlot.value())) {
                        kvSlot = writeKvPair(kvSlot.tag(), readKvPair(kvSlot));
                        leaf.entries[idx] = kvSlot;
                    }
                    var ptr = sortedWriteNode(leaf, nodePtr);
                    return new SortedInsertResult(ptr, node.num, kvSlot, false, null);
                }

                // insert a new entry at idx
                var entry = sortedNewEntry(key);
                var entries = new Slot[BTREE_SLOT_COUNT + 1];
                for (int i = 0; i < entries.length; i++) entries[i] = new Slot();
                System.arraycopy(node.entries, 0, entries, 0, idx);
                entries[idx] = entry.kvSlot();
                System.arraycopy(node.entries, idx, entries, idx + 1, node.num - idx);
                int total = node.num + 1;

                if (total <= BTREE_SLOT_COUNT) {
                    var leaf = new SortedNode(BTreeNodeKind.LEAF, total);
                    System.arraycopy(entries, 0, leaf.entries, 0, total);
                    var ptr = sortedWriteNode(leaf, nodePtr);
                    return new SortedInsertResult(ptr, total, entry.kvSlot(), true, null);
                }

                // overflow: split into two leaves; the new sibling's separator is the
                // key of its first entry
                int leftN = BTREE_SPLIT_COUNT;
                int rightN = total - leftN;
                var left = new SortedNode(BTreeNodeKind.LEAF, leftN);
                System.arraycopy(entries, 0, left.entries, 0, leftN);
                var right = new SortedNode(BTreeNodeKind.LEAF, rightN);
                System.arraycopy(entries, leftN, right.entries, 0, rightN);
                var separator = readKvPair(entries[leftN]).keySlot();
                var leftPtr = sortedWriteNode(left, nodePtr);
                var rightPtr = writeSortedNode(right);
                return new SortedInsertResult(leftPtr, leftN, entry.kvSlot(), true, new SortedSplit(rightPtr, rightN, separator));
            }
            case BRANCH -> {
                int i = 0;
                while (i + 1 < node.num && compareKey(node.separators[i + 1], key) <= 0) i++;
                var child = sortedPut(node.children[i].value(), key);

                var children = new Slot[BTREE_SLOT_COUNT + 1];
                var separators = new Slot[BTREE_SLOT_COUNT + 1];
                var counts = new long[BTREE_SLOT_COUNT + 1];
                for (int k = 0; k < children.length; k++) { children[k] = new Slot(); separators[k] = new Slot(); }
                System.arraycopy(node.children, 0, children, 0, node.num);
                System.arraycopy(node.separators, 0, separators, 0, node.num);
                System.arraycopy(node.counts, 0, counts, 0, node.num);
                children[i] = new Slot(child.nodePtr(), Tag.INDEX);
                counts[i] = child.count();
                int total = node.num;
                if (child.split() != null) {
                    var split = child.split();
                    for (int j = node.num; j > i + 1; j--) {
                        children[j] = children[j - 1];
                        separators[j] = separators[j - 1];
                        counts[j] = counts[j - 1];
                    }
                    children[i + 1] = new Slot(split.nodePtr(), Tag.INDEX);
                    separators[i + 1] = split.separator();
                    counts[i + 1] = split.count();
                    total = node.num + 1;
                }

                if (total <= BTREE_SLOT_COUNT) {
                    var branch = new SortedNode(BTreeNodeKind.BRANCH, total);
                    System.arraycopy(children, 0, branch.children, 0, total);
                    System.arraycopy(separators, 0, branch.separators, 0, total);
                    System.arraycopy(counts, 0, branch.counts, 0, total);
                    var ptr = sortedWriteNode(branch, nodePtr);
                    return new SortedInsertResult(ptr, branch.subtreeCount(), child.kvSlot(), child.added(), null);
                }

                // overflow: split into two branches; the new sibling's separator is the
                // smallest key of its first child (separators[leftN] of the combined)
                int leftN = BTREE_SPLIT_COUNT;
                int rightN = total - leftN;
                var left = new SortedNode(BTreeNodeKind.BRANCH, leftN);
                System.arraycopy(children, 0, left.children, 0, leftN);
                System.arraycopy(separators, 0, left.separators, 0, leftN);
                System.arraycopy(counts, 0, left.counts, 0, leftN);
                var right = new SortedNode(BTreeNodeKind.BRANCH, rightN);
                System.arraycopy(children, leftN, right.children, 0, rightN);
                System.arraycopy(separators, leftN, right.separators, 0, rightN);
                System.arraycopy(counts, leftN, right.counts, 0, rightN);
                var separator = separators[leftN];
                var leftPtr = sortedWriteNode(left, nodePtr);
                var rightPtr = writeSortedNode(right);
                return new SortedInsertResult(leftPtr, left.subtreeCount(), child.kvSlot(), child.added(), new SortedSplit(rightPtr, right.subtreeCount(), separator));
            }
        }
        throw new UnreachableException();
    }

    // remove key from the subtree at nodePtr, path-copying nodes and decrementing
    // counts. an emptied leaf is left in place (see SortedRemoveResult).
    private SortedRemoveResult sortedRemove(long nodePtr, byte[] key) throws IOException {
        var node = readSortedNode(nodePtr);
        switch (node.kind) {
            case LEAF -> {
                int idx = node.num;
                boolean found = false;
                for (int i = 0; i < node.num; i++) {
                    var kv = readKvPair(node.entries[i]);
                    int cmp = compareKey(kv.keySlot(), key);
                    if (cmp == 0) { idx = i; found = true; break; }
                    if (cmp > 0) break;
                }
                if (!found) return new SortedRemoveResult(nodePtr, false);

                var leaf = new SortedNode(BTreeNodeKind.LEAF, node.num - 1);
                System.arraycopy(node.entries, 0, leaf.entries, 0, idx);
                System.arraycopy(node.entries, idx + 1, leaf.entries, idx, node.num - 1 - idx);
                var ptr = sortedWriteNode(leaf, nodePtr);
                return new SortedRemoveResult(ptr, true);
            }
            case BRANCH -> {
                int i = 0;
                while (i + 1 < node.num && compareKey(node.separators[i + 1], key) <= 0) i++;
                var child = sortedRemove(node.children[i].value(), key);
                if (!child.found()) return new SortedRemoveResult(nodePtr, false);

                var branch = node;
                branch.children[i] = new Slot(child.nodePtr(), Tag.INDEX);
                branch.counts[i] -= 1;
                var ptr = sortedWriteNode(branch, nodePtr);
                return new SortedRemoveResult(ptr, true);
            }
        }
        throw new UnreachableException();
    }

    private long sortedGrowRoot(SortedInsertResult result) throws IOException {
        if (result.split() != null) {
            var split = result.split();
            var root = new SortedNode(BTreeNodeKind.BRANCH, 2);
            root.children[0] = new Slot(result.nodePtr(), Tag.INDEX);
            root.children[1] = new Slot(split.nodePtr(), Tag.INDEX);
            root.separators[1] = split.separator(); // separators[0] is an unused sentinel
            root.counts[0] = result.count();
            root.counts[1] = split.count();
            return writeSortedNode(root);
        }
        return result.nodePtr();
    }

    // turn a located/inserted entry into the slot for the requested target. only
    // the value is writeable (that is how put works); the key and the entry
    // pointer are immutable, so they are returned with no writeable position.
    private SlotPointer sortedTargetSlot(Slot kvSlot, SortedMapGetTarget target) throws IOException {
        var kv = readKvPair(kvSlot);
        if (target instanceof SortedMapGetKVPair) {
            return new SlotPointer(null, kvSlot);
        } else if (target instanceof SortedMapGetKey) {
            return new SlotPointer(null, kv.keySlot());
        } else if (target instanceof SortedMapGetValue) {
            return new SlotPointer(kvSlot.value() + kvKeyOffset(kvSlot.tag()) + Slot.length, kv.valueSlot());
        } else {
            throw new IllegalArgumentException();
        }
    }

    // compaction helpers

    @FunctionalInterface
    private interface ObjectPopulator {
        void populate(long sourceOffset, long targetOffset) throws Exception;
    }

    private static class Compactor {
        private final Core sourceCore;
        private final Core targetCore;
        private final short hashSize;
        private final OffsetMap offsetMap;

        private Compactor(Core sourceCore, Core targetCore, short hashSize, OffsetMap offsetMap) {
            this.sourceCore = sourceCore;
            this.targetCore = targetCore;
            this.hashSize = hashSize;
            this.offsetMap = offsetMap;
        }

        private long reserveBlock(int size) throws IOException {
            var offset = targetCore.length();
            targetCore.seek(offset);
            targetCore.writer().write(new byte[size]);
            return offset;
        }

        private long visitObject(long sourceOffset, int size, ObjectPopulator populate) throws Exception {
            var targetOffset = offsetMap.get(sourceOffset);
            if (targetOffset != null) return targetOffset;

            targetOffset = reserveBlock(size);
            offsetMap.put(sourceOffset, targetOffset);
            populate.populate(sourceOffset, targetOffset);
            return targetOffset;
        }

        private Slot remapSlot(Slot slot) throws Exception {
            switch (slot.tag()) {
                case NONE, UINT, INT, FLOAT, SHORT_BYTES -> {
                    return slot;
                }
                case BYTES -> {
                    var newOffset = remapBytes(slot);
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                case INDEX -> {
                    var newOffset = visitObject(slot.value(), INDEX_BLOCK_SIZE, this::populateIndex);
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                case ARRAY_LIST -> {
                    var newOffset = visitObject(slot.value(), ArrayListHeader.length, this::populateArrayList);
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                case LINKED_ARRAY_LIST -> {
                    var newOffset = visitObject(slot.value(), BTreeHeader.length, this::populateBTree);
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                case HASH_MAP, HASH_SET -> {
                    var newOffset = visitObject(slot.value(), INDEX_BLOCK_SIZE, this::populateHashMapOrSet);
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                case COUNTED_HASH_MAP, COUNTED_HASH_SET -> {
                    var newOffset = visitObject(slot.value(), INDEX_BLOCK_SIZE + 8, this::populateCountedHashMapOrSet);
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                case KV_PAIR -> {
                    var newOffset = visitObject(slot.value(), KeyValuePair.length(hashSize),
                        (sourceOffset, targetOffset) -> populateKvPair(sourceOffset, targetOffset, Tag.KV_PAIR));
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                case SORTED_KV_PAIR -> {
                    var newOffset = visitObject(slot.value(), SortedKeyValuePair.length,
                        (sourceOffset, targetOffset) -> populateKvPair(sourceOffset, targetOffset, Tag.SORTED_KV_PAIR));
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                case SORTED_MAP, SORTED_SET -> {
                    var newOffset = visitObject(slot.value(), BTreeHeader.length, this::populateSortedMap);
                    return new Slot(newOffset, slot.tag(), slot.full());
                }
                default -> throw new UnexpectedTagException();
            }
        }

        private long remapBytes(Slot slot) throws Exception {
            var mapped = offsetMap.get(slot.value());
            if (mapped != null) return mapped;

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

            offsetMap.put(slot.value(), newOffset);
            return newOffset;
        }

        private void populateIndex(long sourceOffset, long targetOffset) throws Exception {
            // read 144-byte block (16 slots)
            sourceCore.seek(sourceOffset);
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
                remappedSlots[i] = remapSlot(childSlot);
            }

            // write remapped block to target
            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            for (var s : remappedSlots) {
                targetWriter.write(s.toBytes());
            }
        }

        private long remapArrayListIndex(long sourceOffset, long size, int shift) throws Exception {
            var childSize = 1L << (shift * BIT_COUNT);

            // full blocks can use the normal cache. partial blocks may
            // be shared by lists with different sizes, so copy them
            // separately and leave the slots beyond the size empty.
            if (childSize <= Long.MAX_VALUE / SLOT_COUNT && size == childSize * SLOT_COUNT) {
                return remapSlot(new Slot(sourceOffset, Tag.INDEX)).value();
            }

            var targetOffset = reserveBlock(INDEX_BLOCK_SIZE);
            sourceCore.seek(sourceOffset);
            var blockBytes = new byte[INDEX_BLOCK_SIZE];
            sourceCore.reader().readFully(blockBytes);
            var buffer = ByteBuffer.wrap(blockBytes);
            var remappedBlock = ByteBuffer.allocate(INDEX_BLOCK_SIZE);
            var remaining = size;
            for (int i = 0; i < SLOT_COUNT && remaining > 0; i++) {
                var slotBytes = new byte[Slot.length];
                buffer.get(slotBytes);
                var childSlot = Slot.fromBytes(slotBytes);
                var count = Math.min(remaining, childSize);
                Slot remappedSlot;
                if (shift == 0) {
                    remappedSlot = remapSlot(childSlot);
                } else {
                    if (childSlot.tag() != Tag.INDEX) throw new UnexpectedTagException();
                    var childOffset = remapArrayListIndex(childSlot.value(), count, shift - 1);
                    remappedSlot = new Slot(childOffset, childSlot.tag(), childSlot.full());
                }
                remappedBlock.put(remappedSlot.toBytes());
                remaining -= count;
            }

            targetCore.seek(targetOffset);
            targetCore.writer().write(remappedBlock.array());
            return targetOffset;
        }

        private void populateArrayList(long sourceOffset, long targetOffset) throws Exception {
            // read ArrayListHeader (16 bytes)
            sourceCore.seek(sourceOffset);
            var sourceReader = sourceCore.reader();
            var headerBytes = new byte[ArrayListHeader.length];
            sourceReader.readFully(headerBytes);
            var header = ArrayListHeader.fromBytes(headerBytes);

            var shift = header.size() <= SLOT_COUNT ? 0 : (63 - Long.numberOfLeadingZeros(header.size() - 1)) / BIT_COUNT;
            var remappedIndex = remapArrayListIndex(header.ptr(), header.size(), shift);

            // write new ArrayListHeader with remapped ptr
            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            targetWriter.write(new ArrayListHeader(remappedIndex, header.size()).toBytes());
        }

        private void populateBTree(long sourceOffset, long targetOffset) throws Exception {
            sourceCore.seek(sourceOffset);
            var sourceReader = sourceCore.reader();
            var headerBytes = new byte[BTreeHeader.length];
            sourceReader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            var remappedRoot = remapBTreeNode(header.rootPtr());

            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            targetWriter.write(new BTreeHeader(remappedRoot, header.size()).toBytes());
        }

        private long remapBTreeNode(long nodeOffset) throws Exception {
            // dedup check (subtrees are shared by pointer)
            var mapped = offsetMap.get(nodeOffset);
            if (mapped != null) return mapped;

            // read the header first because the node kind determines its size
            sourceCore.seek(nodeOffset);
            var sourceReader = sourceCore.reader();
            var nodeHeader = new byte[BTREE_NODE_HEADER_SIZE];
            sourceReader.readFully(nodeHeader);
            var kindInt = nodeHeader[0] & 0xFF;
            if (kindInt >= BTreeNodeKind.values().length) throw new InvalidBTreeNodeKindException();
            var kind = BTreeNodeKind.values()[kindInt];
            var num = nodeHeader[1] & 0xFF;
            if (num > BTREE_SLOT_COUNT) throw new InvalidBTreeNodeException();

            return switch (kind) {
                case LEAF -> visitObject(nodeOffset, BTREE_LEAF_BLOCK_SIZE,
                    (sourceOffset, targetOffset) -> populateBTreeLeaf(sourceOffset, targetOffset, kindInt, num));
                case BRANCH -> visitObject(nodeOffset, BTREE_BRANCH_BLOCK_SIZE,
                    (sourceOffset, targetOffset) -> populateBTreeBranch(sourceOffset, targetOffset, kindInt, num));
            };
        }

        private void populateBTreeLeaf(long sourceOffset, long targetOffset, int kind, int num) throws Exception {
            sourceCore.seek(sourceOffset + BTREE_NODE_HEADER_SIZE);
            var sourceReader = sourceCore.reader();
            var body = new byte[Slot.length * BTREE_SLOT_COUNT];
            sourceReader.readFully(body);
            var buffer = ByteBuffer.wrap(body);

            var slots = new Slot[BTREE_SLOT_COUNT];
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                var slotBytes = new byte[Slot.length];
                buffer.get(slotBytes);
                var valueSlot = Slot.fromBytes(slotBytes);
                slots[i] = remapSlot(valueSlot);
            }

            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            targetWriter.writeByte(kind);
            targetWriter.writeByte(num);
            for (var slot : slots) targetWriter.write(slot.toBytes());
        }

        private void populateBTreeBranch(long sourceOffset, long targetOffset, int kind, int num) throws Exception {
            sourceCore.seek(sourceOffset + BTREE_NODE_HEADER_SIZE);
            var sourceReader = sourceCore.reader();
            var body = new byte[(Slot.length + 8) * BTREE_SLOT_COUNT];
            sourceReader.readFully(body);
            var buffer = ByteBuffer.wrap(body);

            var children = new Slot[BTREE_SLOT_COUNT];
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                var slotBytes = new byte[Slot.length];
                buffer.get(slotBytes);
                var child = Slot.fromBytes(slotBytes);
                if (child.tag() == Tag.INDEX) {
                    var remappedPtr = remapBTreeNode(child.value());
                    children[i] = new Slot(remappedPtr, Tag.INDEX, child.full());
                } else {
                    children[i] = child;
                }
            }
            var counts = new long[BTREE_SLOT_COUNT];
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) counts[i] = buffer.getLong();

            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            targetWriter.writeByte(kind);
            targetWriter.writeByte(num);
            for (var child : children) targetWriter.write(child.toBytes());
            for (var count : counts) targetWriter.writeLong(count);
        }

        private void populateSortedMap(long sourceOffset, long targetOffset) throws Exception {
            sourceCore.seek(sourceOffset);
            var sourceReader = sourceCore.reader();
            var headerBytes = new byte[BTreeHeader.length];
            sourceReader.readFully(headerBytes);
            var header = BTreeHeader.fromBytes(headerBytes);

            var remappedRoot = remapSortedMapNode(header.rootPtr());

            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            targetWriter.write(new BTreeHeader(remappedRoot, header.size()).toBytes());
        }

        private long remapSortedMapNode(long nodeOffset) throws Exception {
            var mapped = offsetMap.get(nodeOffset);
            if (mapped != null) return mapped;

            sourceCore.seek(nodeOffset);
            var sourceReader = sourceCore.reader();
            var nodeHeader = new byte[BTREE_NODE_HEADER_SIZE];
            sourceReader.readFully(nodeHeader);
            var kindInt = nodeHeader[0] & 0xFF;
            if (kindInt >= BTreeNodeKind.values().length) throw new InvalidBTreeNodeKindException();
            var kind = BTreeNodeKind.values()[kindInt];
            var num = nodeHeader[1] & 0xFF;
            if (num > BTREE_SLOT_COUNT) throw new InvalidBTreeNodeException();

            return switch (kind) {
                case LEAF -> visitObject(nodeOffset, SORTED_LEAF_BLOCK_SIZE,
                    (sourceOffset, targetOffset) -> populateSortedMapLeaf(sourceOffset, targetOffset, kindInt, num));
                case BRANCH -> visitObject(nodeOffset, SORTED_BRANCH_BLOCK_SIZE,
                    (sourceOffset, targetOffset) -> populateSortedMapBranch(sourceOffset, targetOffset, kindInt, num));
            };
        }

        private void populateSortedMapLeaf(long sourceOffset, long targetOffset, int kind, int num) throws Exception {
            sourceCore.seek(sourceOffset + BTREE_NODE_HEADER_SIZE);
            var sourceReader = sourceCore.reader();
            var body = new byte[Slot.length * BTREE_SLOT_COUNT];
            sourceReader.readFully(body);
            var buffer = ByteBuffer.wrap(body);

            var entries = new Slot[BTREE_SLOT_COUNT];
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                var slotBytes = new byte[Slot.length];
                buffer.get(slotBytes);
                var entry = Slot.fromBytes(slotBytes);
                entries[i] = remapSlot(entry);
            }

            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            targetWriter.writeByte(kind);
            targetWriter.writeByte(num);
            for (var entry : entries) targetWriter.write(entry.toBytes());
        }

        private void populateSortedMapBranch(long sourceOffset, long targetOffset, int kind, int num) throws Exception {
            sourceCore.seek(sourceOffset + BTREE_NODE_HEADER_SIZE);
            var sourceReader = sourceCore.reader();
            var body = new byte[(Slot.length * 2 + 8) * BTREE_SLOT_COUNT];
            sourceReader.readFully(body);
            var buffer = ByteBuffer.wrap(body);

            var children = new Slot[BTREE_SLOT_COUNT];
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                var slotBytes = new byte[Slot.length];
                buffer.get(slotBytes);
                var child = Slot.fromBytes(slotBytes);
                if (child.tag() == Tag.INDEX) {
                    var remappedPtr = remapSortedMapNode(child.value());
                    children[i] = new Slot(remappedPtr, Tag.INDEX, child.full());
                } else {
                    children[i] = child;
                }
            }
            var separators = new Slot[BTREE_SLOT_COUNT];
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) {
                var slotBytes = new byte[Slot.length];
                buffer.get(slotBytes);
                var separator = Slot.fromBytes(slotBytes);
                separators[i] = remapSlot(separator);
            }
            var counts = new long[BTREE_SLOT_COUNT];
            for (int i = 0; i < BTREE_SLOT_COUNT; i++) counts[i] = buffer.getLong();

            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            targetWriter.writeByte(kind);
            targetWriter.writeByte(num);
            for (var child : children) targetWriter.write(child.toBytes());
            for (var separator : separators) targetWriter.write(separator.toBytes());
            for (var count : counts) targetWriter.writeLong(count);
        }

        private void populateHashMapOrSet(long sourceOffset, long targetOffset) throws Exception {
            populateHashMapOrSet(sourceOffset, targetOffset, false);
        }

        private void populateCountedHashMapOrSet(long sourceOffset, long targetOffset) throws Exception {
            populateHashMapOrSet(sourceOffset, targetOffset, true);
        }

        private void populateHashMapOrSet(long sourceOffset, long targetOffset, boolean counted) throws Exception {
            sourceCore.seek(sourceOffset);
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
                remappedSlots[i] = remapSlot(childSlot);
            }

            // write [optional count][remapped block] contiguously to target
            targetCore.seek(targetOffset);
            var targetWriter = targetCore.writer();
            if (counted) {
                targetWriter.writeLong(countValue);
            }
            for (var s : remappedSlots) {
                targetWriter.write(s.toBytes());
            }
        }

        private void populateKvPair(long sourceOffset, long targetOffset, Tag tag) throws Exception {
            sourceCore.seek(sourceOffset);
            var kvPair = decodeKvPair(sourceCore.reader(), tag, hashSize);

            // remap key_slot and value_slot, and write the pair in the same form
            var remappedKey = remapSlot(kvPair.keySlot());
            var remappedValue = remapSlot(kvPair.valueSlot());
            targetCore.seek(targetOffset);
            encodeKvPair(targetCore.writer(), tag, new KeyValuePair(remappedValue, remappedKey, kvPair.hash()));
        }
    }
}
