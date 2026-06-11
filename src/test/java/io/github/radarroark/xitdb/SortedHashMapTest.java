package io.github.radarroark.xitdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class SortedHashMapTest {

    /** Zero-padded decimal keys: unsigned byte order equals numeric order. */
    private static byte[] sortKey(int i) {
        return String.format("%05d", i).getBytes(StandardCharsets.UTF_8);
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
}
