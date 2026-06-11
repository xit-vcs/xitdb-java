package io.github.radarroark.xitdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class SortedHashMapTest {

    /** Zero-padded decimal keys: unsigned byte order equals numeric order. */
    private static byte[] sortKey(int i) {
        return String.format("%06d", i).getBytes(StandardCharsets.UTF_8);
    }

    private interface MapTest {
        void run(WriteSortedHashMap map) throws Exception;
    }

    private static void withMap(MapTest test) throws Exception {
        try (var ram = new RandomAccessMemory()) {
            var core = new CoreMemory(ram);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            var db = new Database(core, hasher);
            var map = new WriteSortedHashMap(db.rootCursor());
            test.run(map);
        }
    }

    /** Inserts values 0..n-1 in shuffled order; value of entry k is Int(k). */
    private static void insertShuffled(WriteSortedHashMap map, int n) throws Exception {
        var order = new ArrayList<Integer>();
        for (int i = 0; i < n; i++) order.add(i);
        Collections.shuffle(order, new java.util.Random(42));
        for (int i : order) {
            map.putCursorBySortKey(sortKey(i)).write(new Database.Int(i));
        }
    }

    private static List<Long> drain(java.util.Iterator<ReadCursor> iter) throws Exception {
        var result = new ArrayList<Long>();
        while (iter.hasNext()) {
            var kvCursor = iter.next();
            var kv = kvCursor.readKeyValuePair();
            result.add(kv.valueCursor.readInt());
        }
        return result;
    }

    private static List<Long> range(int from, int toExclusive, int step) {
        var result = new ArrayList<Long>();
        for (int i = from; step > 0 ? i < toExclusive : i > toExclusive; i += step) {
            result.add((long) i);
        }
        return result;
    }

    @Test
    void iteratorYieldsEntriesInSortKeyOrder() throws Exception {
        withMap(map -> {
            insertShuffled(map, 1000);
            assertEquals(1000, map.count());
            assertEquals(range(0, 1000, 1), drain(map.iterator()));
        });
    }

    @Test
    void emptyMapIteratorIsEmpty() throws Exception {
        withMap(map -> {
            assertFalse(map.iterator().hasNext());
            assertFalse(map.iterator(null, false).hasNext());
            assertFalse(map.iterator(sortKey(5), true).hasNext());
        });
    }

    @Test
    void iteratorFromExistingKeyStartsThere() throws Exception {
        withMap(map -> {
            insertShuffled(map, 1000);
            assertEquals(range(500, 1000, 1), drain(map.iterator(sortKey(500), true)));
        });
    }

    @Test
    void iteratorFromAbsentKeyStartsAtNextGreater() throws Exception {
        withMap(map -> {
            for (int i = 0; i < 1000; i += 2) { // even keys only
                map.putCursorBySortKey(sortKey(i)).write(new Database.Int(i));
            }
            assertEquals(range(502, 1000, 2), drain(map.iterator(sortKey(501), true)));
        });
    }

    @Test
    void iteratorFromPastEndIsEmpty() throws Exception {
        withMap(map -> {
            insertShuffled(map, 100);
            assertFalse(map.iterator(sortKey(99998), true).hasNext());
        });
    }

    @Test
    void reverseIteratorYieldsDescendingOrder() throws Exception {
        withMap(map -> {
            insertShuffled(map, 1000);
            assertEquals(range(999, -1, -1), drain(map.iterator(null, false)));
        });
    }

    @Test
    void reverseIteratorFromExistingKeyStartsThere() throws Exception {
        withMap(map -> {
            insertShuffled(map, 1000);
            assertEquals(range(500, -1, -1), drain(map.iterator(sortKey(500), false)));
        });
    }

    @Test
    void reverseIteratorFromAbsentKeyStartsAtNextSmaller() throws Exception {
        withMap(map -> {
            for (int i = 0; i < 1000; i += 2) {
                map.putCursorBySortKey(sortKey(i)).write(new Database.Int(i));
            }
            assertEquals(range(500, -2, -2), drain(map.iterator(sortKey(501), false)));
        });
    }

    @Test
    void reverseIteratorFromBeforeStartIsEmpty() throws Exception {
        withMap(map -> {
            for (int i = 10; i < 20; i++) {
                map.putCursorBySortKey(sortKey(i)).write(new Database.Int(i));
            }
            assertFalse(map.iterator(sortKey(5), false).hasNext());
        });
    }

    // --- Deletion ---

    @Test
    void removeAbsentKeyReturnsFalseAndKeepsCount() throws Exception {
        withMap(map -> {
            insertShuffled(map, 100);
            assertFalse(map.removeBySortKey(sortKey(12345)));
            assertEquals(100, map.count());
        });
    }

    @Test
    void removeAllEntriesEmptiesMapAndAllowsReinsert() throws Exception {
        withMap(map -> {
            insertShuffled(map, 50);
            for (int i = 0; i < 50; i++) {
                assertTrue(map.removeBySortKey(sortKey(i)), "remove " + i);
            }
            assertEquals(0, map.count());
            assertFalse(map.iterator().hasNext());
            map.putCursorBySortKey(sortKey(7)).write(new Database.Int(7));
            assertEquals(1, map.count());
            assertEquals(List.of(7L), drain(map.iterator()));
        });
    }

    @Test
    void randomizedDeletesMatchTreeMapReference() throws Exception {
        withMap(map -> {
            var reference = new java.util.TreeMap<Integer, Long>();
            var rng = new java.util.Random(7);
            // grow
            while (reference.size() < 2000) {
                int k = rng.nextInt(100_000);
                if (reference.put(k, (long) k) == null) {
                    map.putCursorBySortKey(sortKey(k)).write(new Database.Int(k));
                }
            }
            // interleave deletes of existing keys, absent keys, and re-inserts
            var keys = new ArrayList<>(reference.keySet());
            Collections.shuffle(keys, rng);
            int checkpoint = 0;
            for (int k : keys.subList(0, 1500)) {
                assertTrue(map.removeBySortKey(sortKey(k)));
                reference.remove(k);
                assertFalse(map.removeBySortKey(sortKey(k))); // now absent
                if (rng.nextInt(4) == 0) {
                    int fresh = 100_000 + rng.nextInt(100_000);
                    if (reference.put(fresh, (long) fresh) == null) {
                        map.putCursorBySortKey(sortKey(fresh)).write(new Database.Int(fresh));
                    }
                }
                if (++checkpoint % 500 == 0) {
                    assertEquals(new ArrayList<>(reference.values()), drain(map.iterator()));
                    assertEquals(reference.size(), map.count());
                }
            }
            assertEquals(new ArrayList<>(reference.values()), drain(map.iterator()));
            assertEquals(reference.size(), map.count());
        });
    }

    @Test
    void deleteKeepsBTreeInvariants() throws Exception {
        withMap(map -> {
            insertShuffled(map, 5000);
            var rng = new java.util.Random(11);
            var alive = new ArrayList<Integer>();
            for (int i = 0; i < 5000; i++) alive.add(i);
            Collections.shuffle(alive, rng);
            for (int k : alive.subList(0, 4000)) {
                map.removeBySortKey(sortKey(k));
            }
            checkInvariants(map);
            assertEquals(1000, map.count());
        });
    }

    @Test
    void singleDeleteAppendsOnlyLogarithmicData() throws Exception {
        withMap(map -> {
            insertShuffled(map, 10_000);
            long before = map.cursor.db.core.length();
            assertTrue(map.removeBySortKey(sortKey(5000)));
            long growth = map.cursor.db.core.length() - before;
            // a root-to-leaf path rewrite is a few KB; the old full rebuild was ~100x this
            assertTrue(growth < 32 * 1024, "file grew by " + growth + " bytes for one delete");
        });
    }

    /** Walks the whole tree: entry-count bounds, sorted keys, uniform leaf depth. */
    private static void checkInvariants(ReadSortedHashMap map) throws Exception {
        long rootPos = map.readRootNodePos();
        if (rootPos <= 0) return;
        checkNode(map, rootPos, true, new int[]{-1}, 0);
    }

    private static void checkNode(ReadSortedHashMap map, long nodePos, boolean isRoot,
                                  int[] leafDepth, int depth) throws Exception {
        var node = map.readNode(nodePos);
        int minKeys = ReadSortedHashMap.MAX_KEYS / 2;
        assertTrue(node.nEntries <= ReadSortedHashMap.MAX_KEYS,
            "node has " + node.nEntries + " entries (max " + ReadSortedHashMap.MAX_KEYS + ")");
        if (!isRoot) {
            assertTrue(node.nEntries >= minKeys,
                "non-root node has " + node.nEntries + " entries (min " + minKeys + ")");
        } else {
            assertTrue(node.nEntries >= 1, "root must have at least one entry");
        }
        for (int i = 1; i < node.nEntries; i++) {
            assertTrue(java.util.Arrays.compareUnsigned(node.sortKeys[i - 1], node.sortKeys[i]) < 0,
                "keys out of order within node");
        }
        if (node.isLeaf) {
            if (leafDepth[0] == -1) leafDepth[0] = depth;
            assertEquals(leafDepth[0], depth, "leaves at different depths");
        } else {
            for (int i = 0; i <= node.nEntries; i++) {
                checkNode(map, node.childPositions[i], false, leafDepth, depth + 1);
            }
        }
    }
}
