package io.github.radarroark.xitdb;

import java.io.IOException;
import java.util.Arrays;

/**
 * A read-only sorted hash map implemented as a B-tree ordered by sort key bytes.
 * Sort keys are the raw key bytes (format-tag + value) BEFORE hashing.
 * This gives correct lexicographic ordering for same-type keys (keywords, strings).
 *
 * Extends ReadHashMap so existing operations code can type-hint against it.
 *
 * Storage format:
 *   Root: [count: 8 bytes][root-node-position: 8 bytes]
 *   Node: [is-leaf: 1 byte][n-entries: 2 bytes]
 *         For each entry: [sortKeyLen: 2 bytes][sortKey: sortKeyLen bytes][kvPairPos: 8 bytes]
 *         If internal: [childPos: 8 bytes] * (n+1)
 */
public class ReadSortedHashMap extends ReadHashMap {
    static final int MAX_KEYS = 15;

    protected ReadSortedHashMap() {
    }

    public ReadSortedHashMap(ReadCursor cursor) throws Exception {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, SORTED_HASH_MAP -> {
                this.cursor = cursor;
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    // --- B-tree node ---

    protected static class BTreeNode {
        boolean isLeaf;
        int nEntries;
        byte[][] sortKeys;      // sort key bytes for comparison (variable length)
        long[] kvPairPositions;  // positions of KV_PAIRs
        long[] childPositions;   // positions of child nodes (nEntries+1 for internal)

        /** Binary search by sort key. Returns index if found (>=0), or -(insertionPoint)-1 if not. */
        int search(byte[] sortKey) {
            int lo = 0, hi = nEntries - 1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                int cmp = Arrays.compareUnsigned(sortKeys[mid], sortKey);
                if (cmp < 0) lo = mid + 1;
                else if (cmp > 0) hi = mid - 1;
                else return mid;
            }
            return -(lo) - 1;
        }
    }

    protected BTreeNode readNode(long nodePos) throws IOException {
        var node = new BTreeNode();
        var reader = cursor.db.core.reader();

        cursor.db.core.seek(nodePos);
        node.isLeaf = reader.readByte() != 0;
        node.nEntries = reader.readShort();
        node.sortKeys = new byte[node.nEntries][];
        node.kvPairPositions = new long[node.nEntries];
        for (int i = 0; i < node.nEntries; i++) {
            int keyLen = reader.readShort() & 0xFFFF;
            node.sortKeys[i] = new byte[keyLen];
            reader.readFully(node.sortKeys[i]);
            node.kvPairPositions[i] = reader.readLong();
        }
        if (!node.isLeaf) {
            node.childPositions = new long[node.nEntries + 1];
            for (int i = 0; i <= node.nEntries; i++) {
                node.childPositions[i] = reader.readLong();
            }
        }
        return node;
    }

    protected long readRootNodePos() throws IOException {
        if (cursor.slotPtr.slot().tag() == Tag.NONE) return -1;
        var reader = cursor.db.core.reader();
        cursor.db.core.seek(cursor.slotPtr.slot().value() + 8);
        return reader.readLong();
    }

    /** Find the KV_PAIR position for a given sort key, or -1 if not found. */
    protected long findKVPairPos(byte[] sortKey) throws IOException {
        long rootPos = readRootNodePos();
        if (rootPos <= 0) return -1;

        long nodePos = rootPos;
        while (true) {
            var node = readNode(nodePos);
            int idx = node.search(sortKey);
            if (idx >= 0) {
                return node.kvPairPositions[idx];
            }
            if (node.isLeaf) return -1;
            int childIdx = -(idx) - 1;
            nodePos = node.childPositions[childIdx];
        }
    }

    // --- Lookup by sort key (primary API for sorted maps) ---

    /**
     * Get value cursor by sort key bytes. O(log n) B-tree lookup.
     * Sort key bytes are the pre-hash bytes (format-tag + value).
     */
    public ReadCursor getCursorBySortKey(byte[] sortKey) throws IOException {
        long kvPos = findKVPairPos(sortKey);
        if (kvPos < 0) return null;
        int hashSize = cursor.db.header.hashSize();
        long valueSlotPos = kvPos + hashSize + Slot.length;
        cursor.db.core.seek(valueSlotPos);
        var reader = cursor.db.core.reader();
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        return new ReadCursor(new SlotPointer(valueSlotPos, Slot.fromBytes(slotBytes)), cursor.db);
    }

    /**
     * Get key cursor by sort key bytes. O(log n) B-tree lookup.
     */
    public ReadCursor getKeyCursorBySortKey(byte[] sortKey) throws IOException {
        long kvPos = findKVPairPos(sortKey);
        if (kvPos < 0) return null;
        int hashSize = cursor.db.header.hashSize();
        long keySlotPos = kvPos + hashSize;
        cursor.db.core.seek(keySlotPos);
        var reader = cursor.db.core.reader();
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        return new ReadCursor(new SlotPointer(keySlotPos, Slot.fromBytes(slotBytes)), cursor.db);
    }

    // --- Override inherited getCursor/getKeyCursor to use hash-based scan ---
    // These are O(n) fallbacks for compatibility with operations code.

    @Override
    public ReadCursor getCursor(byte[] keyHash) throws Exception {
        return scanByHash(keyHash, false);
    }

    @Override
    public ReadCursor getKeyCursor(byte[] keyHash) throws Exception {
        return scanByHash(keyHash, true);
    }

    protected ReadCursor scanByHash(byte[] keyHash, boolean returnKey) throws IOException {
        long rootPos = readRootNodePos();
        if (rootPos <= 0) return null;
        return scanNodeByHash(rootPos, keyHash, returnKey);
    }

    protected ReadCursor scanNodeByHash(long nodePos, byte[] keyHash, boolean returnKey) throws IOException {
        var node = readNode(nodePos);
        int hashSize = cursor.db.header.hashSize();

        for (int i = 0; i < node.nEntries; i++) {
            // Read the hash from the KV_PAIR
            long kvPos = node.kvPairPositions[i];
            cursor.db.core.seek(kvPos);
            var reader = cursor.db.core.reader();
            var hash = new byte[hashSize];
            reader.readFully(hash);
            if (Arrays.equals(hash, keyHash)) {
                if (returnKey) {
                    long keySlotPos = kvPos + hashSize;
                    cursor.db.core.seek(keySlotPos);
                    var slotBytes = new byte[Slot.length];
                    reader.readFully(slotBytes);
                    return new ReadCursor(new SlotPointer(keySlotPos, Slot.fromBytes(slotBytes)), cursor.db);
                } else {
                    long valueSlotPos = kvPos + hashSize + Slot.length;
                    cursor.db.core.seek(valueSlotPos);
                    var slotBytes = new byte[Slot.length];
                    reader.readFully(slotBytes);
                    return new ReadCursor(new SlotPointer(valueSlotPos, Slot.fromBytes(slotBytes)), cursor.db);
                }
            }
        }
        if (!node.isLeaf) {
            for (int i = 0; i <= node.nEntries; i++) {
                var result = scanNodeByHash(node.childPositions[i], keyHash, returnKey);
                if (result != null) return result;
            }
        }
        return null;
    }

    // --- Count ---

    public long count() throws IOException {
        if (cursor.slotPtr.slot().tag() == Tag.NONE) return 0;
        var reader = cursor.db.core.reader();
        cursor.db.core.seek(cursor.slotPtr.slot().value());
        return reader.readLong();
    }

    // --- In-order iterator ---

    @Override
    public ReadCursor.Iterator iterator() {
        return iterator(null, true);
    }

    /**
     * Streaming in-order iterator over KV_PAIR cursors.
     *
     * @param startSortKey if non-null, start at the first entry whose sort key is
     *                     >= startSortKey (ascending) or <= startSortKey (descending);
     *                     if null, start at the first/last entry.
     * @param ascending    iteration direction.
     */
    public ReadCursor.Iterator iterator(byte[] startSortKey, boolean ascending) {
        try {
            return new BTreeIterator(this, startSortKey, ascending);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Streams entries with a stack of B-tree node frames, so memory use is
     * O(tree height) regardless of map size. Each frame's idx points at the
     * next entry of that node to emit (entries of internal nodes are emitted
     * between their adjacent child subtrees, per in-order traversal).
     */
    public static class BTreeIterator extends ReadCursor.Iterator {
        private final ReadSortedHashMap map;
        private final boolean ascending;
        private final java.util.ArrayDeque<Frame> stack = new java.util.ArrayDeque<>();

        private static class Frame {
            final BTreeNode node;
            int idx;

            Frame(BTreeNode node, int idx) {
                this.node = node;
                this.idx = idx;
            }
        }

        public BTreeIterator(ReadSortedHashMap map) throws IOException {
            this(map, null, true);
        }

        public BTreeIterator(ReadSortedHashMap map, byte[] startSortKey, boolean ascending) throws IOException {
            super(map.cursor);
            this.map = map;
            this.ascending = ascending;
            long rootPos = map.readRootNodePos();
            if (rootPos > 0) {
                if (startSortKey == null) {
                    pushEdgePath(rootPos);
                } else {
                    seek(rootPos, startSortKey);
                }
                popExhausted();
            }
        }

        /** Descend along the leftmost (ascending) or rightmost (descending) path. */
        private void pushEdgePath(long nodePos) throws IOException {
            while (true) {
                var node = map.readNode(nodePos);
                stack.push(new Frame(node, ascending ? 0 : node.nEntries - 1));
                if (node.isLeaf) return;
                nodePos = ascending ? node.childPositions[0] : node.childPositions[node.nEntries];
            }
        }

        /**
         * Position the stack so the next emitted entry is the first >= startSortKey
         * (ascending) or the last <= startSortKey (descending).
         */
        private void seek(long nodePos, byte[] key) throws IOException {
            while (true) {
                var node = map.readNode(nodePos);
                int idx = node.search(key);
                if (idx >= 0) {
                    // exact match: emit it first in either direction
                    stack.push(new Frame(node, idx));
                    return;
                }
                int insertionPoint = -(idx) - 1;
                // entries[insertionPoint-1] < key < entries[insertionPoint];
                // child[insertionPoint] holds the keys strictly between them
                stack.push(new Frame(node, ascending ? insertionPoint : insertionPoint - 1));
                if (node.isLeaf) return;
                nodePos = node.childPositions[insertionPoint];
            }
        }

        /** Drop finished frames so the top frame (if any) has a valid next entry. */
        private void popExhausted() {
            while (!stack.isEmpty()) {
                var f = stack.peek();
                boolean exhausted = ascending ? f.idx >= f.node.nEntries : f.idx < 0;
                if (!exhausted) return;
                stack.pop();
            }
        }

        @Override
        public boolean hasNext() {
            return !stack.isEmpty();
        }

        @Override
        public ReadCursor next() {
            if (stack.isEmpty()) return null;
            var f = stack.peek();
            long kvPos = f.node.kvPairPositions[f.idx];
            try {
                if (ascending) {
                    f.idx++;
                    if (!f.node.isLeaf) pushEdgePath(f.node.childPositions[f.idx]);
                } else {
                    f.idx--;
                    if (!f.node.isLeaf) pushEdgePath(f.node.childPositions[f.idx + 1]);
                }
                popExhausted();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new ReadCursor(
                new SlotPointer(kvPos, new Slot(kvPos, Tag.KV_PAIR)),
                map.cursor.db);
        }
    }
}
