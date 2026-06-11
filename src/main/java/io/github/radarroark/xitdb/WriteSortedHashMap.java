package io.github.radarroark.xitdb;

import java.io.IOException;
import java.util.Arrays;

/**
 * A writable sorted hash map implemented as a B-tree ordered by sort key bytes.
 * Insert, lookup, and delete are all O(log n); delete rebalances with sibling
 * borrows/merges on the way down, appending only the rewritten path (plus at
 * most one sibling per level) to the file.
 */
public class WriteSortedHashMap extends ReadSortedHashMap {
    private long rootStart;

    public WriteSortedHashMap(WriteCursor cursor) throws Exception {
        this.cursor = cursor;
        var writer = cursor.db.core.writer();
        var reader = cursor.db.core.reader();

        switch (cursor.slotPtr.slot().tag()) {
            case NONE -> {
                rootStart = cursor.db.core.length();
                cursor.db.core.seek(rootStart);
                writer.writeLong(0);  // count = 0
                writer.writeLong(0);  // root node pos = 0
                if (cursor.slotPtr.position() != null) {
                    var newSlot = new Slot(rootStart, Tag.SORTED_HASH_MAP);
                    cursor.db.core.seek(cursor.slotPtr.position());
                    writer.write(newSlot.toBytes());
                    cursor.slotPtr = cursor.slotPtr.withSlot(newSlot);
                }
            }
            case SORTED_HASH_MAP -> {
                rootStart = cursor.slotPtr.slot().value();
                if (cursor.db.txStart != null && rootStart < cursor.db.txStart) {
                    cursor.db.core.seek(rootStart);
                    var headerBytes = new byte[16];
                    reader.readFully(headerBytes);
                    var newStart = cursor.db.core.length();
                    cursor.db.core.seek(newStart);
                    writer.write(headerBytes);
                    rootStart = newStart;
                    if (cursor.slotPtr.position() != null) {
                        var newSlot = new Slot(rootStart, Tag.SORTED_HASH_MAP);
                        cursor.db.core.seek(cursor.slotPtr.position());
                        writer.write(newSlot.toBytes());
                        cursor.slotPtr = cursor.slotPtr.withSlot(newSlot);
                    }
                }
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    @Override
    protected long readRootNodePos() throws IOException {
        var reader = cursor.db.core.reader();
        cursor.db.core.seek(rootStart + 8);
        return reader.readLong();
    }

    @Override
    public long count() throws IOException {
        var reader = cursor.db.core.reader();
        cursor.db.core.seek(rootStart);
        return reader.readLong();
    }

    private void writeCount(long count) throws IOException {
        var writer = cursor.db.core.writer();
        cursor.db.core.seek(rootStart);
        writer.writeLong(count);
    }

    private void writeRootNodePos(long pos) throws IOException {
        var writer = cursor.db.core.writer();
        cursor.db.core.seek(rootStart + 8);
        writer.writeLong(pos);
    }

    // --- Node write helpers ---

    private long writeNode(BTreeNode node) throws IOException {
        var writer = cursor.db.core.writer();
        long pos = cursor.db.core.length();
        cursor.db.core.seek(pos);
        writer.writeByte(node.isLeaf ? 1 : 0);
        writer.writeShort(node.nEntries);
        for (int i = 0; i < node.nEntries; i++) {
            writer.writeShort(node.sortKeys[i].length);
            writer.write(node.sortKeys[i]);
            writer.writeLong(node.kvPairPositions[i]);
        }
        if (!node.isLeaf) {
            for (int i = 0; i <= node.nEntries; i++) {
                writer.writeLong(node.childPositions[i]);
            }
        }
        return pos;
    }

    private void overwriteNode(long pos, BTreeNode node) throws IOException {
        var writer = cursor.db.core.writer();
        cursor.db.core.seek(pos);
        writer.writeByte(node.isLeaf ? 1 : 0);
        writer.writeShort(node.nEntries);
        for (int i = 0; i < node.nEntries; i++) {
            writer.writeShort(node.sortKeys[i].length);
            writer.write(node.sortKeys[i]);
            writer.writeLong(node.kvPairPositions[i]);
        }
        if (!node.isLeaf) {
            for (int i = 0; i <= node.nEntries; i++) {
                writer.writeLong(node.childPositions[i]);
            }
        }
    }

    private long createKVPair(byte[] sortKey) throws IOException {
        // Hash the sort key to get the key hash for the KV_PAIR
        var hash = cursor.db.md.digest(sortKey);
        var writer = cursor.db.core.writer();
        long pos = cursor.db.core.length();
        cursor.db.core.seek(pos);
        var kvPair = new Database.KeyValuePair(new Slot(), new Slot(), hash);
        writer.write(kvPair.toBytes());
        return pos;
    }

    private long ensureWritable(long nodePos) throws IOException {
        if (cursor.db.txStart != null && nodePos < cursor.db.txStart) {
            var node = readNode(nodePos);
            return writeNode(node);
        }
        return nodePos;
    }

    // --- Insert ---

    private void splitChild(long parentPos, BTreeNode parent, int childIdx) throws IOException {
        long childPos = parent.childPositions[childIdx];
        childPos = ensureWritable(childPos);
        parent.childPositions[childIdx] = childPos;
        var child = readNode(childPos);

        int mid = child.nEntries / 2;

        // Right node: child[mid+1 .. n-1]
        var right = new BTreeNode();
        right.isLeaf = child.isLeaf;
        right.nEntries = child.nEntries - mid - 1;
        right.sortKeys = new byte[right.nEntries][];
        right.kvPairPositions = new long[right.nEntries];
        for (int i = 0; i < right.nEntries; i++) {
            right.sortKeys[i] = child.sortKeys[mid + 1 + i];
            right.kvPairPositions[i] = child.kvPairPositions[mid + 1 + i];
        }
        if (!child.isLeaf) {
            right.childPositions = new long[right.nEntries + 1];
            for (int i = 0; i <= right.nEntries; i++) {
                right.childPositions[i] = child.childPositions[mid + 1 + i];
            }
        }
        long rightPos = writeNode(right);

        // Median
        byte[] medianSortKey = child.sortKeys[mid];
        long medianKvPos = child.kvPairPositions[mid];

        // Shrink child to [0..mid-1]
        child.nEntries = mid;
        overwriteNode(childPos, child);

        // Insert median into parent
        var newSortKeys = new byte[parent.nEntries + 1][];
        var newKvPos = new long[parent.nEntries + 1];
        var newChildren = new long[parent.nEntries + 2];
        for (int i = 0; i < childIdx; i++) {
            newSortKeys[i] = parent.sortKeys[i];
            newKvPos[i] = parent.kvPairPositions[i];
            newChildren[i] = parent.childPositions[i];
        }
        newSortKeys[childIdx] = medianSortKey;
        newKvPos[childIdx] = medianKvPos;
        newChildren[childIdx] = childPos;
        newChildren[childIdx + 1] = rightPos;
        for (int i = childIdx; i < parent.nEntries; i++) {
            newSortKeys[i + 1] = parent.sortKeys[i];
            newKvPos[i + 1] = parent.kvPairPositions[i];
            newChildren[i + 2] = parent.childPositions[i + 1];
        }
        parent.nEntries++;
        parent.sortKeys = newSortKeys;
        parent.kvPairPositions = newKvPos;
        parent.childPositions = newChildren;
        // Parent has grown — must rewrite at a new position (size changed)
        long newParentPos = writeNode(parent);
        // Caller needs to know the new position — we update in place via the return mechanism
        // Actually, overwriteNode won't work if the node grew. We need to write to a new position.
        // Let's handle this by always writing to new positions for parent after split.
    }

    private static class InsertResult {
        long kvPairPos;
        boolean isNew;
    }

    /**
     * Insert sort key into a non-full node. The node at nodePos must not be full.
     * Returns [kvPairPos, isNew, newNodePos] where newNodePos may differ from nodePos
     * if the node was rewritten.
     */
    private long insertResultKvPos;
    private boolean insertResultIsNew;

    private long insertNonFull(long nodePos, byte[] sortKey) throws IOException {
        var node = readNode(nodePos);
        int idx = node.search(sortKey);

        if (idx >= 0) {
            insertResultKvPos = node.kvPairPositions[idx];
            insertResultIsNew = false;
            return nodePos;
        }

        int insertPos = -(idx) - 1;

        if (node.isLeaf) {
            long kvPos = createKVPair(sortKey);
            var newSortKeys = new byte[node.nEntries + 1][];
            var newKvPos = new long[node.nEntries + 1];
            for (int i = 0; i < insertPos; i++) {
                newSortKeys[i] = node.sortKeys[i];
                newKvPos[i] = node.kvPairPositions[i];
            }
            newSortKeys[insertPos] = sortKey;
            newKvPos[insertPos] = kvPos;
            for (int i = insertPos; i < node.nEntries; i++) {
                newSortKeys[i + 1] = node.sortKeys[i];
                newKvPos[i + 1] = node.kvPairPositions[i];
            }
            node.nEntries++;
            node.sortKeys = newSortKeys;
            node.kvPairPositions = newKvPos;
            // Leaf grew — write to new position
            long newPos = writeNode(node);
            insertResultKvPos = kvPos;
            insertResultIsNew = true;
            return newPos;
        }

        // Internal: ensure child is writable and not full
        long childPos = node.childPositions[insertPos];
        childPos = ensureWritable(childPos);
        node.childPositions[insertPos] = childPos;

        var child = readNode(childPos);
        if (child.nEntries >= MAX_KEYS) {
            // Split child — this modifies node
            splitChild(nodePos, node, insertPos);
            // Node was modified by splitChild. Since it grew, write to new pos.
            nodePos = writeNode(node);
            // Determine which child to recurse into
            int cmp = Arrays.compareUnsigned(sortKey, node.sortKeys[insertPos]);
            if (cmp == 0) {
                insertResultKvPos = node.kvPairPositions[insertPos];
                insertResultIsNew = false;
                return nodePos;
            } else if (cmp > 0) {
                insertPos++;
            }
            childPos = node.childPositions[insertPos];
        }

        long newChildPos = insertNonFull(childPos, sortKey);
        if (newChildPos != childPos) {
            node.childPositions[insertPos] = newChildPos;
            nodePos = writeNode(node);
        }
        return nodePos;
    }

    /**
     * Put a value into the sorted map by sort key. Returns WriteCursor to value slot.
     */
    public WriteCursor putCursorBySortKey(byte[] sortKey) throws Exception {
        int hashSize = cursor.db.header.hashSize();
        long rootNodePos = readRootNodePos();

        if (rootNodePos <= 0) {
            long kvPos = createKVPair(sortKey);
            var root = new BTreeNode();
            root.isLeaf = true;
            root.nEntries = 1;
            root.sortKeys = new byte[][]{sortKey};
            root.kvPairPositions = new long[]{kvPos};
            long newRootPos = writeNode(root);
            writeRootNodePos(newRootPos);
            writeCount(1);

            long valueSlotPos = kvPos + hashSize + Slot.length;
            cursor.db.core.seek(valueSlotPos);
            var reader = cursor.db.core.reader();
            var slotBytes = new byte[Slot.length];
            reader.readFully(slotBytes);
            return new WriteCursor(new SlotPointer(valueSlotPos, Slot.fromBytes(slotBytes)), cursor.db);
        }

        rootNodePos = ensureWritable(rootNodePos);
        writeRootNodePos(rootNodePos);

        var root = readNode(rootNodePos);
        if (root.nEntries >= MAX_KEYS) {
            var newRoot = new BTreeNode();
            newRoot.isLeaf = false;
            newRoot.nEntries = 0;
            newRoot.sortKeys = new byte[0][];
            newRoot.kvPairPositions = new long[0];
            newRoot.childPositions = new long[]{rootNodePos};
            long newRootPos = writeNode(newRoot);
            writeRootNodePos(newRootPos);
            splitChild(newRootPos, newRoot, 0);
            newRootPos = writeNode(newRoot);
            writeRootNodePos(newRootPos);
            rootNodePos = newRootPos;
        }

        long newRootPos = insertNonFull(rootNodePos, sortKey);
        if (newRootPos != rootNodePos) {
            writeRootNodePos(newRootPos);
        }
        if (insertResultIsNew) {
            writeCount(count() + 1);
        }

        long valueSlotPos = insertResultKvPos + hashSize + Slot.length;
        cursor.db.core.seek(valueSlotPos);
        var reader = cursor.db.core.reader();
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        return new WriteCursor(new SlotPointer(valueSlotPos, Slot.fromBytes(slotBytes)), cursor.db);
    }

    /**
     * Put a key into the sorted map by sort key. Returns WriteCursor to key slot.
     */
    public WriteCursor putKeyCursorBySortKey(byte[] sortKey) throws Exception {
        putCursorBySortKey(sortKey); // ensure entry exists
        long kvPos = findKVPairPos(sortKey);
        int hashSize = cursor.db.header.hashSize();
        long keySlotPos = kvPos + hashSize;
        cursor.db.core.seek(keySlotPos);
        var reader = cursor.db.core.reader();
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        return new WriteCursor(new SlotPointer(keySlotPos, Slot.fromBytes(slotBytes)), cursor.db);
    }

    // --- Delete (standard top-down B-tree deletion, O(log n)) ---
    //
    // Every modified node is appended via writeNode (copy-on-write), so only
    // the root-to-leaf path plus at most one sibling per level is rewritten.
    // Nodes from earlier transactions are never touched in place.

    static final int MIN_KEYS = MAX_KEYS / 2;

    /**
     * Remove by sort key. O(log n): rebalances with sibling borrows/merges
     * on the way down so no node ever underflows.
     */
    public boolean removeBySortKey(byte[] sortKey) throws Exception {
        long kvPos = findKVPairPos(sortKey);
        if (kvPos < 0) return false;

        long currentCount = count();
        long newRootPos = deleteFrom(readRootNodePos(), sortKey);
        var newRoot = readNode(newRootPos);
        if (newRoot.nEntries == 0) {
            // root emptied: a leaf root means the map is now empty
            newRootPos = newRoot.isLeaf ? 0 : newRoot.childPositions[0];
        }
        writeRootNodePos(newRootPos);
        writeCount(currentCount - 1);
        return true;
    }

    private record Entry(byte[] sortKey, long kvPos) {}

    private Entry maxEntry(long nodePos) throws IOException {
        while (true) {
            var n = readNode(nodePos);
            if (n.isLeaf) return new Entry(n.sortKeys[n.nEntries - 1], n.kvPairPositions[n.nEntries - 1]);
            nodePos = n.childPositions[n.nEntries];
        }
    }

    private Entry minEntry(long nodePos) throws IOException {
        while (true) {
            var n = readNode(nodePos);
            if (n.isLeaf) return new Entry(n.sortKeys[0], n.kvPairPositions[0]);
            nodePos = n.childPositions[0];
        }
    }

    /**
     * Deletes `key` from the subtree at nodePos and returns the position of
     * the rewritten subtree root. The caller guarantees the key exists and
     * that this node has more than MIN_KEYS entries (or is the root).
     */
    private long deleteFrom(long nodePos, byte[] key) throws IOException {
        var node = readNode(nodePos);
        int idx = node.search(key);

        if (node.isLeaf) {
            if (idx < 0) return nodePos; // defensive; existence is pre-checked
            removeEntry(node, idx);
            return writeNode(node);
        }

        if (idx >= 0) {
            // key sits in this internal node: replace it with its in-order
            // neighbor from whichever adjacent child can spare an entry
            var left = readNode(node.childPositions[idx]);
            if (left.nEntries > MIN_KEYS) {
                var pred = maxEntry(node.childPositions[idx]);
                node.sortKeys[idx] = pred.sortKey();
                node.kvPairPositions[idx] = pred.kvPos();
                node.childPositions[idx] = deleteFrom(node.childPositions[idx], pred.sortKey());
                return writeNode(node);
            }
            var right = readNode(node.childPositions[idx + 1]);
            if (right.nEntries > MIN_KEYS) {
                var succ = minEntry(node.childPositions[idx + 1]);
                node.sortKeys[idx] = succ.sortKey();
                node.kvPairPositions[idx] = succ.kvPos();
                node.childPositions[idx + 1] = deleteFrom(node.childPositions[idx + 1], succ.sortKey());
                return writeNode(node);
            }
            // both neighbors minimal: merge them around the key, then delete
            // the key from the merged child
            long mergedPos = mergeChildren(node, idx);
            if (node.nEntries == 0) {
                return deleteFrom(mergedPos, key); // root collapse
            }
            node.childPositions[idx] = deleteFrom(mergedPos, key);
            return writeNode(node);
        }

        // key is below: make sure the target child can lose an entry, then descend
        int ip = -(idx) - 1;
        int ci = fixChild(node, ip);
        if (node.nEntries == 0) {
            return deleteFrom(node.childPositions[ci], key); // root collapse via merge
        }
        node.childPositions[ci] = deleteFrom(node.childPositions[ci], key);
        return writeNode(node);
    }

    /**
     * Ensures the child at index i has more than MIN_KEYS entries, borrowing
     * from a sibling or merging with one. Returns the index of the child to
     * descend into (i-1 if the child was merged into its left sibling).
     */
    private int fixChild(BTreeNode parent, int i) throws IOException {
        var child = readNode(parent.childPositions[i]);
        if (child.nEntries > MIN_KEYS) return i;

        if (i > 0) {
            var left = readNode(parent.childPositions[i - 1]);
            if (left.nEntries > MIN_KEYS) {
                // rotate right: separator moves down to child's front,
                // left sibling's last entry moves up to the separator slot
                insertEntry(child, 0, parent.sortKeys[i - 1], parent.kvPairPositions[i - 1]);
                if (!child.isLeaf) {
                    child.childPositions = insertAt(child.childPositions, 0, left.childPositions[left.nEntries]);
                }
                parent.sortKeys[i - 1] = left.sortKeys[left.nEntries - 1];
                parent.kvPairPositions[i - 1] = left.kvPairPositions[left.nEntries - 1];
                if (!left.isLeaf) {
                    left.childPositions = dropAt(left.childPositions, left.nEntries);
                }
                removeEntry(left, left.nEntries - 1);
                parent.childPositions[i - 1] = writeNode(left);
                parent.childPositions[i] = writeNode(child);
                return i;
            }
        }
        if (i < parent.nEntries) {
            var right = readNode(parent.childPositions[i + 1]);
            if (right.nEntries > MIN_KEYS) {
                // rotate left: separator moves down to child's end,
                // right sibling's first entry moves up to the separator slot
                insertEntry(child, child.nEntries, parent.sortKeys[i], parent.kvPairPositions[i]);
                if (!child.isLeaf) {
                    child.childPositions = insertAt(child.childPositions, child.nEntries, right.childPositions[0]);
                }
                parent.sortKeys[i] = right.sortKeys[0];
                parent.kvPairPositions[i] = right.kvPairPositions[0];
                removeEntry(right, 0);
                if (!right.isLeaf) {
                    right.childPositions = dropAt(right.childPositions, 0);
                }
                parent.childPositions[i] = writeNode(child);
                parent.childPositions[i + 1] = writeNode(right);
                return i;
            }
        }
        // both siblings minimal: merge (two MIN_KEYS nodes + separator = MAX_KEYS)
        if (i > 0) {
            mergeChildren(parent, i - 1);
            return i - 1;
        }
        mergeChildren(parent, 0);
        return 0;
    }

    /**
     * Merges parent's children at sepIdx and sepIdx+1 with the separator entry
     * between them. Removes the separator and the right child from the parent
     * (in memory) and points parent.childPositions[sepIdx] at the merged node.
     */
    private long mergeChildren(BTreeNode parent, int sepIdx) throws IOException {
        var left = readNode(parent.childPositions[sepIdx]);
        var right = readNode(parent.childPositions[sepIdx + 1]);
        var merged = new BTreeNode();
        merged.isLeaf = left.isLeaf;
        merged.nEntries = left.nEntries + 1 + right.nEntries;
        merged.sortKeys = new byte[merged.nEntries][];
        merged.kvPairPositions = new long[merged.nEntries];
        System.arraycopy(left.sortKeys, 0, merged.sortKeys, 0, left.nEntries);
        System.arraycopy(left.kvPairPositions, 0, merged.kvPairPositions, 0, left.nEntries);
        merged.sortKeys[left.nEntries] = parent.sortKeys[sepIdx];
        merged.kvPairPositions[left.nEntries] = parent.kvPairPositions[sepIdx];
        System.arraycopy(right.sortKeys, 0, merged.sortKeys, left.nEntries + 1, right.nEntries);
        System.arraycopy(right.kvPairPositions, 0, merged.kvPairPositions, left.nEntries + 1, right.nEntries);
        if (!left.isLeaf) {
            merged.childPositions = new long[merged.nEntries + 1];
            System.arraycopy(left.childPositions, 0, merged.childPositions, 0, left.nEntries + 1);
            System.arraycopy(right.childPositions, 0, merged.childPositions, left.nEntries + 1, right.nEntries + 1);
        }
        long mergedPos = writeNode(merged);
        removeEntry(parent, sepIdx);
        parent.childPositions = dropAt(parent.childPositions, sepIdx + 1);
        parent.childPositions[sepIdx] = mergedPos;
        return mergedPos;
    }

    // --- In-memory node entry manipulation ---

    private static void removeEntry(BTreeNode n, int i) {
        n.sortKeys = dropAt(n.sortKeys, i);
        n.kvPairPositions = dropAt(n.kvPairPositions, i);
        n.nEntries--;
    }

    private static void insertEntry(BTreeNode n, int i, byte[] key, long kvPos) {
        n.sortKeys = insertAt(n.sortKeys, i, key);
        n.kvPairPositions = insertAt(n.kvPairPositions, i, kvPos);
        n.nEntries++;
    }

    private static byte[][] insertAt(byte[][] a, int i, byte[] v) {
        var r = new byte[a.length + 1][];
        System.arraycopy(a, 0, r, 0, i);
        r[i] = v;
        System.arraycopy(a, i, r, i + 1, a.length - i);
        return r;
    }

    private static long[] insertAt(long[] a, int i, long v) {
        var r = new long[a.length + 1];
        System.arraycopy(a, 0, r, 0, i);
        r[i] = v;
        System.arraycopy(a, i, r, i + 1, a.length - i);
        return r;
    }

    private static byte[][] dropAt(byte[][] a, int i) {
        var r = new byte[a.length - 1][];
        System.arraycopy(a, 0, r, 0, i);
        System.arraycopy(a, i + 1, r, i, a.length - i - 1);
        return r;
    }

    private static long[] dropAt(long[] a, int i) {
        var r = new long[a.length - 1];
        System.arraycopy(a, 0, r, 0, i);
        System.arraycopy(a, i + 1, r, i, a.length - i - 1);
        return r;
    }

    // --- Convenience overrides for hash-based API ---
    // These are O(n) since they scan by hash. Use sortKey methods for O(log n).

    public WriteCursor putCursor(byte[] keyHash) throws Exception {
        // Find existing entry by hash scan
        var existing = scanByHashWrite(keyHash);
        if (existing != null) return existing;
        // Can't insert by hash alone — need sort key. This shouldn't be called
        // for new entries. Throw to catch misuse.
        throw new IllegalArgumentException(
            "putCursor(hash) cannot insert new entries in SortedHashMap. Use putCursorBySortKey(sortKey).");
    }

    public WriteCursor putKeyCursor(byte[] keyHash) throws Exception {
        var existing = scanByHashWriteKey(keyHash);
        if (existing != null) return existing;
        throw new IllegalArgumentException(
            "putKeyCursor(hash) cannot insert new entries in SortedHashMap. Use putKeyCursorBySortKey(sortKey).");
    }

    private WriteCursor scanByHashWrite(byte[] keyHash) throws IOException {
        var readCursor = scanByHash(keyHash, false);
        if (readCursor == null) return null;
        return new WriteCursor(readCursor.slotPtr, cursor.db);
    }

    private WriteCursor scanByHashWriteKey(byte[] keyHash) throws IOException {
        var readCursor = scanByHash(keyHash, true);
        if (readCursor == null) return null;
        return new WriteCursor(readCursor.slotPtr, cursor.db);
    }

    public boolean remove(byte[] keyHash) throws Exception {
        // Find the sort key for this hash, then remove by sort key
        long rootPos = readRootNodePos();
        if (rootPos <= 0) return false;
        byte[] sortKey = findSortKeyByHash(rootPos, keyHash);
        if (sortKey == null) return false;
        return removeBySortKey(sortKey);
    }

    private byte[] findSortKeyByHash(long nodePos, byte[] keyHash) throws IOException {
        var node = readNode(nodePos);
        int hashSize = cursor.db.header.hashSize();
        for (int i = 0; i < node.nEntries; i++) {
            long kvPos = node.kvPairPositions[i];
            cursor.db.core.seek(kvPos);
            var reader = cursor.db.core.reader();
            var hash = new byte[hashSize];
            reader.readFully(hash);
            if (Arrays.equals(hash, keyHash)) {
                return node.sortKeys[i];
            }
        }
        if (!node.isLeaf) {
            for (int i = 0; i <= node.nEntries; i++) {
                var result = findSortKeyByHash(node.childPositions[i], keyHash);
                if (result != null) return result;
            }
        }
        return null;
    }
}
