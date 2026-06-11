package io.github.radarroark.xitdb;

import java.io.IOException;
import java.util.Arrays;

/**
 * A writable sorted hash map implemented as a B-tree ordered by sort key bytes.
 * Supports O(log n) insert and lookup. Delete rebuilds the tree (O(n) for now).
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

    /**
     * Remove by sort key. Rebuilds tree without the entry (O(n)).
     */
    public boolean removeBySortKey(byte[] sortKey) throws Exception {
        long kvPos = findKVPairPos(sortKey);
        if (kvPos < 0) return false;

        long currentCount = count();
        var allSortKeys = new java.util.ArrayList<byte[]>();
        var allKvPositions = new java.util.ArrayList<Long>();
        collectForRebuild(readRootNodePos(), sortKey, allSortKeys, allKvPositions);

        // Build new tree from collected entries
        if (allSortKeys.isEmpty()) {
            writeRootNodePos(0);
        } else {
            long newRoot = buildLeafFromEntries(allSortKeys, allKvPositions, 0, allSortKeys.size() - 1);
            writeRootNodePos(newRoot);
        }
        writeCount(currentCount - 1);
        return true;
    }

    private void collectForRebuild(long nodePos, byte[] excludeSortKey,
                                    java.util.List<byte[]> sortKeys, java.util.List<Long> kvPositions) throws IOException {
        var node = readNode(nodePos);
        if (node.isLeaf) {
            for (int i = 0; i < node.nEntries; i++) {
                if (!Arrays.equals(node.sortKeys[i], excludeSortKey)) {
                    sortKeys.add(node.sortKeys[i]);
                    kvPositions.add(node.kvPairPositions[i]);
                }
            }
        } else {
            for (int i = 0; i < node.nEntries; i++) {
                collectForRebuild(node.childPositions[i], excludeSortKey, sortKeys, kvPositions);
                if (!Arrays.equals(node.sortKeys[i], excludeSortKey)) {
                    sortKeys.add(node.sortKeys[i]);
                    kvPositions.add(node.kvPairPositions[i]);
                }
            }
            collectForRebuild(node.childPositions[node.nEntries], excludeSortKey, sortKeys, kvPositions);
        }
    }

    private long buildLeafFromEntries(java.util.List<byte[]> sortKeys, java.util.List<Long> kvPositions, int lo, int hi) throws IOException {
        int n = hi - lo + 1;
        if (n <= MAX_KEYS) {
            var node = new BTreeNode();
            node.isLeaf = true;
            node.nEntries = n;
            node.sortKeys = new byte[n][];
            node.kvPairPositions = new long[n];
            for (int i = 0; i < n; i++) {
                node.sortKeys[i] = sortKeys.get(lo + i);
                node.kvPairPositions[i] = kvPositions.get(lo + i);
            }
            return writeNode(node);
        }

        // Split into chunks and build internal nodes
        int numChunks = (n + MAX_KEYS - 1) / MAX_KEYS;
        if (numChunks < 2) numChunks = 2;
        var leafPositions = new long[numChunks];
        int entriesPerChunk = n / numChunks;
        int remainder = n % numChunks;
        int pos = lo;
        var chunkLastSortKeys = new byte[numChunks][];
        var chunkLastKvPositions = new long[numChunks];
        for (int c = 0; c < numChunks; c++) {
            int size = entriesPerChunk + (c < remainder ? 1 : 0);
            var node = new BTreeNode();
            node.isLeaf = true;
            node.nEntries = size;
            node.sortKeys = new byte[size][];
            node.kvPairPositions = new long[size];
            for (int i = 0; i < size; i++) {
                node.sortKeys[i] = sortKeys.get(pos + i);
                node.kvPairPositions[i] = kvPositions.get(pos + i);
            }
            chunkLastSortKeys[c] = node.sortKeys[size - 1];
            chunkLastKvPositions[c] = node.kvPairPositions[size - 1];
            leafPositions[c] = writeNode(node);
            pos += size;
        }

        // Build internal node pulling last entry from each chunk as separator
        int numSeps = numChunks - 1;
        var internal = new BTreeNode();
        internal.isLeaf = false;
        internal.nEntries = numSeps;
        internal.sortKeys = new byte[numSeps][];
        internal.kvPairPositions = new long[numSeps];
        internal.childPositions = new long[numChunks];
        for (int i = 0; i < numSeps; i++) {
            internal.sortKeys[i] = chunkLastSortKeys[i];
            internal.kvPairPositions[i] = chunkLastKvPositions[i];
            // Shrink this leaf by removing last entry
            var leaf = readNode(leafPositions[i]);
            leaf.nEntries--;
            leafPositions[i] = writeNode(leaf);
            internal.childPositions[i] = leafPositions[i];
        }
        internal.childPositions[numChunks - 1] = leafPositions[numChunks - 1];
        return writeNode(internal);
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
