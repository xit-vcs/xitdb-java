package io.github.radarroark.xitdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class HighLevelDatabaseTest {
    static long MAX_READ_BYTES = 1024;

    @Test
    void testHightLevelApi() throws Exception {
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            testHighLevelApi(core, hasher, null);
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();

            try (var core = new CoreFile(new RandomAccessFile(file, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testHighLevelApi(core, hasher, file);
            }
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();

            try (var core = new CoreBufferedFile(new RandomAccessBufferedFile(file, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testHighLevelApi(core, hasher, file);
            }
        }
    }

    @Test
    void notUsingArrayListAtTopLevel() throws Exception {
        // normally an arraylist makes the most sense at the top level,
        // but this test just ensures we can use other data structures
        // at the top level. in theory a top-level hash map might make
        // sense if we're using xitdb as a format to send data over a
        // network. in that case, immutability isn't important because
        // the data is just created and immediately sent over the wire.

        // hash map
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            var db = new Database(core, hasher);

            var map = new WriteHashMap(db.rootCursor());
            map.put("foo", new Database.Bytes("foo"));
            map.put("bar", new Database.Bytes("bar"));

            // every write to the root is a transaction, so a nested
            // cursor can't be written to after it was returned
            var innerMapCursor = map.putCursor("inner-map");
            assertThrows(IllegalStateException.class, () -> new WriteHashMap(innerMapCursor));
        }

        // linked array list
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            var db = new Database(core, hasher);

            var list = new WriteLinkedArrayList(db.rootCursor());
            list.append(new Database.Uint(1));
            assertEquals(1, list.count());
        }
    }

    @Test
    void testReadDatabaseFromResources() throws Exception {
        var resource = getClass().getClassLoader().getResource("test.db");
        File file = new File(resource.toURI());
        try (var core = new CoreFile(new RandomAccessFile(file, "r"))) {
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            var db = new Database(core, hasher);
            var history = new ReadArrayList(db.rootCursor());

            {
                var momentCursor = history.getCursor(0);
                var moment = new ReadHashMap(momentCursor);

                var fooCursor = moment.getCursor("foo");
                var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
                assertEquals("foo", new String(fooValue));

                assertEquals(Tag.SHORT_BYTES, moment.getSlot("foo").tag());
                assertEquals(Tag.SHORT_BYTES, moment.getSlot("bar").tag());

                var fruitsCursor = moment.getCursor("fruits");
                var fruits = new ReadArrayList(fruitsCursor);
                assertEquals(3, fruits.count());

                var appleCursor = fruits.getCursor(0);
                var appleValue = appleCursor.readBytes(MAX_READ_BYTES);
                assertEquals("apple", new String(appleValue));

                var peopleCursor = moment.getCursor("people");
                var people = new ReadArrayList(peopleCursor);
                assertEquals(2, people.count());

                var aliceCursor = people.getCursor(0);
                var alice = new ReadHashMap(aliceCursor);
                var aliceAgeCursor = alice.getCursor("age");
                assertEquals(25, aliceAgeCursor.readUint());

                var todosCursor = moment.getCursor("todos");
                var todos = new ReadLinkedArrayList(todosCursor);
                assertEquals(3, todos.count());

                var todoCursor = todos.getCursor(0);
                var todoValue = todoCursor.readBytes(MAX_READ_BYTES);
                assertEquals("Pay the bills", new String(todoValue));

                var peopleIter = people.iterator();
                while (peopleIter.hasNext()) {
                    var personCursor = peopleIter.next();
                    var person = new ReadHashMap(personCursor);
                    var personIter = person.iterator();
                    while (personIter.hasNext()) {
                        var kvPairCursor = personIter.next();
                        kvPairCursor.readKeyValuePair();
                    }
                }

                {
                    var lettersCountedMapCursor = moment.getCursor("letters-counted-map");
                    var lettersCountedMap = new ReadCountedHashMap(lettersCountedMapCursor);
                    assertEquals(2, lettersCountedMap.count());

                    var iter = lettersCountedMap.iterator();
                    int count = 0;
                    while (iter.hasNext()) {
                        var kvPairCursor = iter.next();
                        var kvPair = kvPairCursor.readKeyValuePair();
                        kvPair.keyCursor.readBytes(MAX_READ_BYTES);
                        count += 1;
                    }
                    assertEquals(2, count);
                }

                {
                    var lettersSetCursor = moment.getCursor("letters-set");
                    var lettersSet = new ReadHashSet(lettersSetCursor);
                    assert(null != lettersSet.getCursor("a"));
                    assert(null != lettersSet.getCursor("c"));

                    var iter = lettersSet.iterator();
                    int count = 0;
                    while (iter.hasNext()) {
                        var kvPairCursor = iter.next();
                        var kvPair = kvPairCursor.readKeyValuePair();
                        kvPair.keyCursor.readBytes(MAX_READ_BYTES);
                        count += 1;
                    }
                    assertEquals(2, count);
                }

                {
                    var lettersCountedSetCursor = moment.getCursor("letters-counted-set");
                    var lettersCountedSet = new ReadCountedHashSet(lettersCountedSetCursor);
                    assertEquals(2, lettersCountedSet.count());

                    var iter = lettersCountedSet.iterator();
                    int count = 0;
                    while (iter.hasNext()) {
                        var kvPairCursor = iter.next();
                        var kvPair = kvPairCursor.readKeyValuePair();
                        kvPair.keyCursor.readBytes(MAX_READ_BYTES);
                        count += 1;
                    }
                    assertEquals(2, count);
                }
            }

            {
                var momentCursor = history.getCursor(1);
                var moment = new ReadHashMap(momentCursor);

                assertEquals(null, moment.getCursor("bar"));

                var fruitsKeyCursor = moment.getKeyCursor("fruits");
                var fruitsKeyValue = fruitsKeyCursor.readBytes(MAX_READ_BYTES);
                assertEquals("fruits", new String(fruitsKeyValue));

                var fruitsCursor = moment.getCursor("fruits");
                var fruits = new ReadArrayList(fruitsCursor);
                assertEquals(2, fruits.count());

                var fruitsKVCursor = moment.getKeyValuePair("fruits");
                assertEquals(Tag.SHORT_BYTES, fruitsKVCursor.keyCursor.slotPtr.slot().tag());
                assertEquals(Tag.ARRAY_LIST, fruitsKVCursor.valueCursor.slotPtr.slot().tag());

                var lemonCursor = fruits.getCursor(0);
                var lemonValue = lemonCursor.readBytes(MAX_READ_BYTES);
                assertEquals("lemon", new String(lemonValue));

                var peopleCursor = moment.getCursor("people");
                var people = new ReadArrayList(peopleCursor);
                assertEquals(2, people.count());

                var aliceCursor = people.getCursor(0);
                var alice = new ReadHashMap(aliceCursor);
                var aliceAgeCursor = alice.getCursor("age");
                assertEquals(26, aliceAgeCursor.readUint());

                var todosCursor = moment.getCursor("todos");
                var todos = new ReadLinkedArrayList(todosCursor);
                assertEquals(1, todos.count());

                var todoCursor = todos.getCursor(0);
                var todoValue = todoCursor.readBytes(MAX_READ_BYTES);
                assertEquals("Wash the car", new String(todoValue));

                var lettersCountedMapCursor = moment.getCursor("letters-counted-map");
                var lettersCountedMap = new ReadCountedHashMap(lettersCountedMapCursor);
                assertEquals(1, lettersCountedMap.count());

                var lettersSetCursor = moment.getCursor("letters-set");
                var lettersSet = new ReadHashSet(lettersSetCursor);
                assert(null != lettersSet.getCursor("a"));
                assert(null == lettersSet.getCursor("c"));

                var lettersCountedSetCursor = moment.getCursor("letters-counted-set");
                var lettersCountedSet = new ReadCountedHashSet(lettersCountedSetCursor);
                assertEquals(1, lettersCountedSet.count());
            }
        }
    }

    @Test
    void testMultithreading() throws Exception {
        var resource = getClass().getClassLoader().getResource("test.db");
        File file = new File(resource.toURI());

        // to read the db from multiple threads, you must create a ThreadLocal
        // and override `initialValue` to open the file and return a Database object.
        // this will ensure that each thread uses a different file handle and
        // Database object. this will only work with reading...writing to the db
        // can only happen from a single thread.
        var db = new ThreadLocal<Database>() {
            @Override
            protected Database initialValue() {
                try {
                    var core = new CoreFile(new RandomAccessFile(file, "r"));
                    var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                    return new Database(core, hasher);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        var t1 = new Thread() {
            @Override
            public void run() {
                try {
                    var history = new ReadArrayList(db.get().rootCursor());
                    var momentCursor = history.getCursor(0);
                    var moment = new ReadHashMap(momentCursor);
                    var fooCursor = moment.getCursor("foo");
                    var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
                    assertEquals("foo", new String(fooValue));
                    // close the db file
                    db.get().core.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        var t2 = new Thread() {
            @Override
            public void run() {
                try {
                    var history = new ReadArrayList(db.get().rootCursor());
                    var momentCursor = history.getCursor(0);
                    var moment = new ReadHashMap(momentCursor);
                    var fooCursor = moment.getCursor("foo");
                    var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
                    assertEquals("foo", new String(fooValue));
                    // close the db file
                    db.get().core.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        // this will move the read position of the db because
        // we need to navigate to the hash map
        var history = new ReadArrayList(db.get().rootCursor());
        var momentCursor = history.getCursor(0);
        var moment = new ReadHashMap(momentCursor);

        // start the threads. since they are using their own file
        // handle, the read position changed above won't affect them
        t1.start();
        t2.start();

        // this should succeed because the threads started above are
        // using their own file handle and db object
        var fooCursor = moment.getCursor("foo");
        var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
        assertEquals("foo", new String(fooValue));

        // wait for the threads to finish
        t1.join();
        t2.join();

        // close the db file
        db.get().core.close();
    }

    void testHighLevelApi(Core core, Hasher hasher, File fileMaybe) throws Exception {
        // init the db
        core.setLength(0);
        var db = new Database(core, hasher);

        {
            // to get the benefits of immutability, the top-level data structure
            // must be an ArrayList, so each transaction is stored as an item in it
            var history = new WriteArrayList(db.rootCursor());

            // this is how a transaction is executed. we call history.appendContext,
            // providing it with the most recent copy of the db and a context
            // object. the context object has a method that will run before the
            // transaction has completed. this method is where we can write
            // changes to the db. if any error happens in it, the transaction
            // will not complete and the db will be unaffected.
            history.appendContext(history.getSlot(-1), (cursor) -> {
                var moment = new WriteHashMap(cursor);

                moment.put("foo", new Database.Bytes("foo"));
                moment.put("bar", new Database.Bytes("bar"));

                var fruitsCursor = moment.putCursor("fruits");
                var fruits = new WriteArrayList(fruitsCursor);
                fruits.append(new Database.Bytes("apple"));
                fruits.append(new Database.Bytes("pear"));
                fruits.append(new Database.Bytes("grape"));

                var peopleCursor = moment.putCursor("people");
                var people = new WriteArrayList(peopleCursor);

                var aliceCursor = people.appendCursor();
                var alice = new WriteHashMap(aliceCursor);
                alice.put("name", new Database.Bytes("Alice"));
                alice.put("age", new Database.Uint(25));

                var bobCursor = people.appendCursor();
                var bob = new WriteHashMap(bobCursor);
                bob.put("name", new Database.Bytes("Bob"));
                bob.put("age", new Database.Uint(42));

                var todosCursor = moment.putCursor("todos");
                var todos = new WriteLinkedArrayList(todosCursor);
                todos.append(new Database.Bytes("Pay the bills"));
                todos.append(new Database.Bytes("Get an oil change"));
                todos.insert(1, new Database.Bytes("Wash the car"));

                // make sure `insertCursor` works as well
                var todoCursor = todos.insertCursor(1);
                new WriteHashMap(todoCursor);
                todos.remove(1);

                var lettersCountedMapCursor = moment.putCursor("letters-counted-map");
                var lettersCountedMap = new WriteCountedHashMap(lettersCountedMapCursor);
                lettersCountedMap.put("a", new Database.Uint(1));
                lettersCountedMap.put("a", new Database.Uint(2));
                lettersCountedMap.put("c", new Database.Uint(2));

                var lettersSetCursor = moment.putCursor("letters-set");
                var lettersSet = new WriteHashSet(lettersSetCursor);
                lettersSet.put("a");
                lettersSet.put("a");
                lettersSet.put("c");

                var lettersCountedSetCursor = moment.putCursor("letters-counted-set");
                var lettersCountedSet = new WriteCountedHashSet(lettersCountedSetCursor);
                lettersCountedSet.put("a");
                lettersCountedSet.put("a");
                lettersCountedSet.put("c");

                var randomBigInt = new BigInteger(256, new java.util.Random());
                moment.put("random-number", new Database.Bytes(randomBigInt.toByteArray(), "bi".getBytes()));

                var longTextCursor = moment.putCursor("long-text");
                var cursorWriter = longTextCursor.writer();
                try (var bos = new BufferedOutputStream(cursorWriter)) {
                    for (int i = 0; i < 50; i++) {
                        bos.write("hello, world\n".getBytes());
                    }
                }
                cursorWriter.finish();
            });

            // get the most recent copy of the database, like a moment
            // in time. the -1 index will return the last index in the list.
            var momentCursor = history.getCursor(-1);
            var moment = new ReadHashMap(momentCursor);

            // we can read the value of "foo" from the map by getting
            // the cursor to "foo" and then calling readBytes on it
            var fooCursor = moment.getCursor("foo");
            var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
            assertEquals("foo", new String(fooValue));

            assertEquals(Tag.SHORT_BYTES, moment.getSlot("foo").tag());
            assertEquals(Tag.SHORT_BYTES, moment.getSlot("bar").tag());

            // to get the "fruits" list, we get the cursor to it and
            // then pass it to the ArrayList constructor
            var fruitsCursor = moment.getCursor("fruits");
            var fruits = new ReadArrayList(fruitsCursor);
            assertEquals(3, fruits.count());

            // now we can get the first item from the fruits list and read it
            var appleCursor = fruits.getCursor(0);
            var appleValue = appleCursor.readBytes(MAX_READ_BYTES);
            assertEquals("apple", new String(appleValue));

            var peopleCursor = moment.getCursor("people");
            var people = new ReadArrayList(peopleCursor);
            assertEquals(2, people.count());

            var aliceCursor = people.getCursor(0);
            var alice = new ReadHashMap(aliceCursor);
            var aliceAgeCursor = alice.getCursor("age");
            assertEquals(25, aliceAgeCursor.readUint());

            var todosCursor = moment.getCursor("todos");
            var todos = new ReadLinkedArrayList(todosCursor);
            assertEquals(3, todos.count());

            var todoCursor = todos.getCursor(0);
            var todoValue = todoCursor.readBytes(MAX_READ_BYTES);
            assertEquals("Pay the bills", new String(todoValue));

            var peopleIter = people.iterator();
            while (peopleIter.hasNext()) {
                var personCursor = peopleIter.next();
                var person = new ReadHashMap(personCursor);
                var personIter = person.iterator();
                while (personIter.hasNext()) {
                    var kvPairCursor = personIter.next();
                    var kvPair = kvPairCursor.readKeyValuePair();

                    kvPair.keyCursor.readBytes(MAX_READ_BYTES);

                    switch (kvPair.valueCursor.slot().tag()) {
                        case SHORT_BYTES, BYTES -> kvPair.valueCursor.readBytes(MAX_READ_BYTES);
                        case UINT -> kvPair.valueCursor.readUint();
                        case INT -> kvPair.valueCursor.readInt();
                        case FLOAT -> kvPair.valueCursor.readFloat();
                        default -> throw new Database.UnexpectedTagException();
                    }
                }
            }

            var fruitsIter = fruits.iterator();
            while (fruitsIter.hasNext()) {
                fruitsIter.next();
            }

            {
                var lettersCountedMapCursor = moment.getCursor("letters-counted-map");
                var lettersCountedMap = new ReadCountedHashMap(lettersCountedMapCursor);
                assertEquals(2, lettersCountedMap.count());

                var iter = lettersCountedMap.iterator();
                int count = 0;
                while (iter.hasNext()) {
                    var kvPairCursor = iter.next();
                    var kvPair = kvPairCursor.readKeyValuePair();
                    kvPair.keyCursor.readBytes(MAX_READ_BYTES);
                    count += 1;
                }
                assertEquals(2, count);
            }

            {
                var lettersSetCursor = moment.getCursor("letters-set");
                var lettersSet = new ReadHashSet(lettersSetCursor);
                assert(null != lettersSet.getCursor("a"));
                assert(null != lettersSet.getCursor("c"));

                var iter = lettersSet.iterator();
                int count = 0;
                while (iter.hasNext()) {
                    var kvPairCursor = iter.next();
                    var kvPair = kvPairCursor.readKeyValuePair();
                    kvPair.keyCursor.readBytes(MAX_READ_BYTES);
                    count += 1;
                }
                assertEquals(2, count);
            }

            {
                var lettersCountedSetCursor = moment.getCursor("letters-counted-set");
                var lettersCountedSet = new ReadCountedHashSet(lettersCountedSetCursor);
                assertEquals(2, lettersCountedSet.count());

                var iter = lettersCountedSet.iterator();
                int count = 0;
                while (iter.hasNext()) {
                    var kvPairCursor = iter.next();
                    var kvPair = kvPairCursor.readKeyValuePair();
                    kvPair.keyCursor.readBytes(MAX_READ_BYTES);
                    count += 1;
                }
                assertEquals(2, count);
            }

            {
                var randomNumberCursor = moment.getCursor("random-number");
                var randomNumber = randomNumberCursor.readBytesObject(MAX_READ_BYTES);
                assertEquals("bi", new String(randomNumber.formatTag()));
            }

            {
                var longTextCursor = moment.getCursor("long-text");
                var cursorReader = longTextCursor.reader();
                var bis = new BufferedInputStream(cursorReader);
                var br = new BufferedReader(new InputStreamReader(bis, StandardCharsets.UTF_8));
                int count = 0;
                for (var it = br.lines().iterator(); it.hasNext();) {
                    it.next();
                    count += 1;
                }
                assertEquals(50, count);
            }
        }

        // make a new transaction and change the data
        {
            var history = new WriteArrayList(db.rootCursor());

            history.appendContext(history.getSlot(-1), (cursor) -> {
                var moment = new WriteHashMap(cursor);

                assert(moment.remove("bar"));
                assert(!moment.remove("doesn't exist"));

                var fruitsCursor = moment.putCursor("fruits");
                var fruits = new WriteArrayList(fruitsCursor);
                fruits.put(0, new Database.Bytes("lemon"));
                fruits.slice(2);

                var peopleCursor = moment.putCursor("people");
                var people = new WriteArrayList(peopleCursor);

                var aliceCursor = people.putCursor(0);
                var alice = new WriteHashMap(aliceCursor);
                alice.put("age", new Database.Uint(26));

                var todosCursor = moment.putCursor("todos");
                var todos = new WriteLinkedArrayList(todosCursor);
                todos.concat(todosCursor.slot());
                todos.slice(1, 2);
                todos.remove(1);

                var lettersCountedMapCursor = moment.putCursor("letters-counted-map");
                var lettersCountedMap = new WriteCountedHashMap(lettersCountedMapCursor);
                lettersCountedMap.remove("b");
                lettersCountedMap.remove("c");

                var lettersSetCursor = moment.putCursor("letters-set");
                var lettersSet = new WriteHashSet(lettersSetCursor);
                lettersSet.remove("b");
                lettersSet.remove("c");

                var lettersCountedSetCursor = moment.putCursor("letters-counted-set");
                var lettersCountedSet = new WriteCountedHashSet(lettersCountedSetCursor);
                lettersCountedSet.remove("b");
                lettersCountedSet.remove("c");
            });

            var momentCursor = history.getCursor(-1);
            var moment = new ReadHashMap(momentCursor);

            assertEquals(null, moment.getCursor("bar"));

            var fruitsKeyCursor = moment.getKeyCursor("fruits");
            var fruitsKeyValue = fruitsKeyCursor.readBytes(MAX_READ_BYTES);
            assertEquals("fruits", new String(fruitsKeyValue));

            var fruitsCursor = moment.getCursor("fruits");
            var fruits = new ReadArrayList(fruitsCursor);
            assertEquals(2, fruits.count());

            // you can get both the key and value cursor this way
            var fruitsKVCursor = moment.getKeyValuePair("fruits");
            assertEquals(Tag.SHORT_BYTES, fruitsKVCursor.keyCursor.slotPtr.slot().tag());
            assertEquals(Tag.ARRAY_LIST, fruitsKVCursor.valueCursor.slotPtr.slot().tag());

            var lemonCursor = fruits.getCursor(0);
            var lemonValue = lemonCursor.readBytes(MAX_READ_BYTES);
            assertEquals("lemon", new String(lemonValue));

            var peopleCursor = moment.getCursor("people");
            var people = new ReadArrayList(peopleCursor);
            assertEquals(2, people.count());

            var aliceCursor = people.getCursor(0);
            var alice = new ReadHashMap(aliceCursor);
            var aliceAgeCursor = alice.getCursor("age");
            assertEquals(26, aliceAgeCursor.readUint());

            var todosCursor = moment.getCursor("todos");
            var todos = new ReadLinkedArrayList(todosCursor);
            assertEquals(1, todos.count());

            var todoCursor = todos.getCursor(0);
            var todoValue = todoCursor.readBytes(MAX_READ_BYTES);
            assertEquals("Wash the car", new String(todoValue));

            var lettersCountedMapCursor = moment.getCursor("letters-counted-map");
            var lettersCountedMap = new ReadCountedHashMap(lettersCountedMapCursor);
            assertEquals(1, lettersCountedMap.count());

            var lettersSetCursor = moment.getCursor("letters-set");
            var lettersSet = new ReadHashSet(lettersSetCursor);
            assert(null != lettersSet.getCursor("a"));
            assert(null == lettersSet.getCursor("c"));

            var lettersCountedSetCursor = moment.getCursor("letters-counted-set");
            var lettersCountedSet = new ReadCountedHashSet(lettersCountedSetCursor);
            assertEquals(1, lettersCountedSet.count());
        }

        // the old data hasn't changed
        {
            var history = new WriteArrayList(db.rootCursor());

            var momentCursor = history.getCursor(0);
            var moment = new ReadHashMap(momentCursor);

            var fooCursor = moment.getCursor("foo");
            var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
            assertEquals("foo", new String(fooValue));

            assertEquals(Tag.SHORT_BYTES, moment.getSlot("foo").tag());
            assertEquals(Tag.SHORT_BYTES, moment.getSlot("bar").tag());

            var fruitsCursor = moment.getCursor("fruits");
            var fruits = new ReadArrayList(fruitsCursor);
            assertEquals(3, fruits.count());

            var appleCursor = fruits.getCursor(0);
            var appleValue = appleCursor.readBytes(MAX_READ_BYTES);
            assertEquals("apple", new String(appleValue));

            var peopleCursor = moment.getCursor("people");
            var people = new ReadArrayList(peopleCursor);
            assertEquals(2, people.count());

            var aliceCursor = people.getCursor(0);
            var alice = new ReadHashMap(aliceCursor);
            var aliceAgeCursor = alice.getCursor("age");
            assertEquals(25, aliceAgeCursor.readUint());

            var todosCursor = moment.getCursor("todos");
            var todos = new ReadLinkedArrayList(todosCursor);
            assertEquals(3, todos.count());

            var todoCursor = todos.getCursor(0);
            var todoValue = todoCursor.readBytes(MAX_READ_BYTES);
            assertEquals("Pay the bills", new String(todoValue));
        }

        // remove the last transaction with `slice`
        {
            var history = new WriteArrayList(db.rootCursor());

            history.slice(1);

            var momentCursor = history.getCursor(-1);
            var moment = new ReadHashMap(momentCursor);

            var fooCursor = moment.getCursor("foo");
            var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
            assertEquals("foo", new String(fooValue));

            assertEquals(Tag.SHORT_BYTES, moment.getSlot("foo").tag());
            assertEquals(Tag.SHORT_BYTES, moment.getSlot("bar").tag());

            var fruitsCursor = moment.getCursor("fruits");
            var fruits = new ReadArrayList(fruitsCursor);
            assertEquals(3, fruits.count());

            var appleCursor = fruits.getCursor(0);
            var appleValue = appleCursor.readBytes(MAX_READ_BYTES);
            assertEquals("apple", new String(appleValue));

            var peopleCursor = moment.getCursor("people");
            var people = new ReadArrayList(peopleCursor);
            assertEquals(2, people.count());

            var aliceCursor = people.getCursor(0);
            var alice = new ReadHashMap(aliceCursor);
            var aliceAgeCursor = alice.getCursor("age");
            assertEquals(25, aliceAgeCursor.readUint());

            var todosCursor = moment.getCursor("todos");
            var todos = new ReadLinkedArrayList(todosCursor);
            assertEquals(3, todos.count());

            var todoCursor = todos.getCursor(0);
            var todoValue = todoCursor.readBytes(MAX_READ_BYTES);
            assertEquals("Pay the bills", new String(todoValue));
        }

        // opening the db leaves trailing data alone, because it may
        // belong to another writer's unfinished transaction. the next
        // write transaction truncates it before allocating new data.
        {
            var sizeBefore = core.length();
            core.seek(sizeBefore);
            var writer = core.writer();
            writer.write("this is trailing data from an unfinished transaction".getBytes());
            core.flush();
            var sizeWithTail = core.length();

            // no error is thrown if db file is opened in read-only mode
            if (fileMaybe != null) {
                try (var readOnlyCore = new CoreFile(new RandomAccessFile(fileMaybe, "r"))) {
                    new Database(readOnlyCore, hasher);
                }
            }

            db = new Database(core, hasher);

            var sizeAfter = core.length();

            assertEquals(sizeWithTail, sizeAfter);
            new WriteArrayList(db.rootCursor());
            assertEquals(sizeBefore, core.length());
        }

        // cloning
        {
            var history = new WriteArrayList(db.rootCursor());

            history.appendContext(history.getSlot(-1), (cursor) -> {
                var moment = new WriteHashMap(cursor);

                var fruitsCursor = moment.getCursor("fruits");
                var fruits = new ReadArrayList(fruitsCursor);

                // create a new key called "food" whose initial value is
                // based on the "fruits" list
                var foodCursor = moment.putCursor("food");
                foodCursor.write(fruits.slot());

                var food = new WriteArrayList(foodCursor);
                food.append(new Database.Bytes("eggs"));
                food.append(new Database.Bytes("rice"));
                food.append(new Database.Bytes("fish"));
            });

            var momentCursor = history.getCursor(-1);
            var moment = new ReadHashMap(momentCursor);

            // the food list includes the fruits
            var foodCursor = moment.getCursor("food");
            var food = new ReadArrayList(foodCursor);
            assertEquals(6, food.count());

            // ...but the fruits list hasn't been changed
            var fruitsCursor = moment.getCursor("fruits");
            var fruits = new ReadArrayList(fruitsCursor);
            assertEquals(3, fruits.count());
        }

        // accidental mutation when cloning inside a transaction
        {
            var history = new WriteArrayList(db.rootCursor());

            var historyIndex = history.count() - 1;

            history.appendContext(history.getSlot(-1), (cursor) -> {
                var moment = new WriteHashMap(cursor);

                var bigCitiesCursor = moment.putCursor("big-cities");
                var bigCities = new WriteArrayList(bigCitiesCursor);
                bigCities.append(new Database.Bytes("New York, NY"));
                bigCities.append(new Database.Bytes("Los Angeles, CA"));

                // create a new key called "cities" whose initial value is
                // based on the "big-cities" list
                var citiesCursor = moment.putCursor("cities");
                citiesCursor.write(bigCities.slot());

                var cities = new WriteArrayList(citiesCursor);
                cities.append(new Database.Bytes("Charleston, SC"));
                cities.append(new Database.Bytes("Louisville, KY"));
            });

            var momentCursor = history.getCursor(-1);
            var moment = new ReadHashMap(momentCursor);

            // the cities list contains all four
            var citiesCursor = moment.getCursor("cities");
            var cities = new ReadArrayList(citiesCursor);
            assertEquals(4, cities.count());

            // ..but so does big-cities! we did not intend to mutate this
            var bigCitiesCursor = moment.getCursor("big-cities");
            var bigCities = new ReadArrayList(bigCitiesCursor);
            assertEquals(4, bigCities.count());

            // revert that change
            history.append(history.getSlot(historyIndex));
        }

        // preventing accidental mutation with freezing
        {
            var history = new WriteArrayList(db.rootCursor());

            history.appendContext(history.getSlot(-1), (cursor) -> {
                var moment = new WriteHashMap(cursor);

                var bigCitiesCursor = moment.putCursor("big-cities");
                var bigCities = new WriteArrayList(bigCitiesCursor);
                bigCities.append(new Database.Bytes("New York, NY"));
                bigCities.append(new Database.Bytes("Los Angeles, CA"));

                // create a new key called "cities" whose initial value is
                // based on the "big-cities" list
                var citiesCursor = moment.putCursor("cities");
                citiesCursor.write(bigCities.slot());

                // freeze here, so writes to cities won't mutate big-cities
                // note: you can also freeze globally with `cursor.db.freeze()`,
                // but call it before `putCursor` creates the cities cursor
                citiesCursor.freeze();

                var cities = new WriteArrayList(citiesCursor);
                cities.append(new Database.Bytes("Charleston, SC"));
                cities.append(new Database.Bytes("Louisville, KY"));
            });

            var momentCursor = history.getCursor(-1);
            var moment = new ReadHashMap(momentCursor);

            // the cities list contains all four
            var citiesCursor = moment.getCursor("cities");
            var cities = new ReadArrayList(citiesCursor);
            assertEquals(4, cities.count());

            // and big-cities only contains the original two
            var bigCitiesCursor = moment.getCursor("big-cities");
            var bigCities = new ReadArrayList(bigCitiesCursor);
            assertEquals(2, bigCities.count());
        }

        // build a secondary index with a SortedMap to sort and paginate,
        // like the "Sorting and Paginating" section of the readme
        {
            var history = new WriteArrayList(db.rootCursor());

            record User(String id, String username, String name) {}

            // inserted in arbitrary order; the index will sort them alphabetically
            var newUsers = List.of(
                new User("user000000000001", "dave", "Dave Smith"),
                new User("user000000000002", "alice", "Alice Jones"),
                new User("user000000000003", "carol", "Carol White"),
                new User("user000000000004", "dan", "Dan Brown"),
                new User("user000000000005", "bob", "Bob Lee"),
                new User("user000000000006", "eve", "Eve Adams"));

            history.appendContext(history.getSlot(-1), (cursor) -> {
                var moment = new WriteHashMap(cursor);

                // the primary store: a HashMap from user id to the user's fields
                var idToUserCursor = moment.putCursor("id->user");
                var idToUser = new WriteHashMap(idToUserCursor);

                // the secondary index: a SortedMap ordered alphabetically by username
                var usernameToIdCursor = moment.putCursor("username->id");
                var usernameToId = new WriteSortedMap(usernameToIdCursor);

                for (var user : newUsers) {
                    var userCursor = idToUser.putCursor(user.id());
                    var userMap = new WriteHashMap(userCursor);
                    userMap.put("username", new Database.Bytes(user.username()));
                    userMap.put("name", new Database.Bytes(user.name()));

                    // the key is the username (the sort key), and the value is
                    // the user id we'll use to look it back up
                    usernameToId.put(user.username(), new Database.Bytes(user.id()));
                }
            });

            var momentCursor = history.getCursor(-1);
            var moment = new ReadHashMap(momentCursor);

            var idToUserCursor = moment.getCursor("id->user");
            var idToUser = new ReadHashMap(idToUserCursor);

            var usernameToIdCursor = moment.getCursor("username->id");
            var usernameToId = new ReadSortedMap(usernameToIdCursor);

            assertEquals(newUsers.size(), usernameToId.count());

            // page through the index two at a time, alphabetically by username,
            // and check we get every user back in alphabetical order
            long pageSize = 2;
            var expectedNames = List.of(
                "Alice Jones", "Bob Lee", "Carol White", "Dan Brown", "Dave Smith", "Eve Adams");

            long count = usernameToId.count();
            int seen = 0;
            for (long after = 0; after < count; after += pageSize) {
                long end = Math.min(after + pageSize, count);
                var iter = usernameToId.iteratorFromIndex(after);
                for (long i = after; i < end && iter.hasNext(); i++) {
                    var idCursor = iter.next();
                    var idKv = idCursor.readKeyValuePair();

                    // the index entry's value is the user id; use it to read the
                    // full user out of the primary map
                    var userId = new String(idKv.valueCursor.readBytes(MAX_READ_BYTES));

                    var userCursor = idToUser.getCursor(userId);
                    var userMap = new ReadHashMap(userCursor);
                    var nameCursor = userMap.getCursor("name");
                    var name = new String(nameCursor.readBytes(MAX_READ_BYTES));
                    assertEquals(expectedNames.get(seen), name);
                    seen += 1;
                }
            }
            assertEquals(newUsers.size(), seen);

            // type-ahead autocomplete: seek to the first username >= "da", then
            // walk forward only as far as the prefix matches (yields "dan", "dave")
            var prefix = "da";
            var expectedUsernames = List.of("dan", "dave");
            int acSeen = 0;
            var acIter = usernameToId.iteratorFrom(prefix);
            while (acIter.hasNext()) {
                var idCursor = acIter.next();
                var idKv = idCursor.readKeyValuePair();

                // the key is the username; stop once we've walked past the prefix
                var username = new String(idKv.keyCursor.readBytes(MAX_READ_BYTES));
                if (!username.startsWith(prefix)) break;

                assertEquals(expectedUsernames.get(acSeen), username);
                acSeen += 1;
            }
            assertEquals(expectedUsernames.size(), acSeen);
        }
    }

    @Test
    void testCompaction() throws Exception {
        // memory
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            try (var targetCore = new CoreMemory(new RandomAccessMemory())) {
                testCompaction(core, targetCore, hasher, null, null, null);
            }
        }

        // file
        {
            var sourceFile = File.createTempFile("compact_source", ".db");
            sourceFile.deleteOnExit();
            var targetFile = File.createTempFile("compact_target", ".db");
            targetFile.deleteOnExit();

            try (var sourceCore = new CoreFile(new RandomAccessFile(sourceFile, "rw"));
                 var targetCore = new CoreFile(new RandomAccessFile(targetFile, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testCompaction(sourceCore, targetCore, hasher, sourceFile, targetFile, null);
            }
        }

        // buffered file
        {
            var sourceFile = File.createTempFile("compact_source", ".db");
            sourceFile.deleteOnExit();
            var targetFile = File.createTempFile("compact_target", ".db");
            targetFile.deleteOnExit();

            try (var sourceCore = new CoreBufferedFile(new RandomAccessBufferedFile(sourceFile, "rw"));
                 var targetCore = new CoreBufferedFile(new RandomAccessBufferedFile(targetFile, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testCompaction(sourceCore, targetCore, hasher, sourceFile, targetFile, null);
            }
        }

        // buffered file to memory
        {
            var sourceFile = File.createTempFile("compact_source", ".db");
            sourceFile.deleteOnExit();

            try (var sourceCore = new CoreBufferedFile(new RandomAccessBufferedFile(sourceFile, "rw"));
                 var targetCore = new CoreMemory(new RandomAccessMemory())) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testCompaction(sourceCore, targetCore, hasher, sourceFile, null, null);
            }
        }
    }

    @Test
    void testCompactionWithDiskBackedOffsetsMap(@TempDir Path tempDir) throws Exception {
        // a custom offsets map only needs reset, get, and put. FileOffsetMap stores
        // offsets directly as hashes in a separate xitdb hash map, inside one
        // transaction that lasts for the whole compaction, so each put isn't
        // committed separately.
        var offsetsFile = tempDir.resolve("compact_offsets.db").toFile();
        var sourceFile = tempDir.resolve("compact_source.db").toFile();
        var targetFile = tempDir.resolve("compact_target.db").toFile();

        try (var offsetsCore = new CoreFile(new RandomAccessFile(offsetsFile, "rw"));
             var sourceCore = new CoreFile(new RandomAccessFile(sourceFile, "rw"));
             var targetCore = new CoreFile(new RandomAccessFile(targetFile, "rw"))) {
            var offsetsDb = new Database(offsetsCore, new Hasher(MessageDigest.getInstance("SHA-256")));
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));

            offsetsDb.runContext(cursor -> {
                var offsetMap = new FileOffsetMap(new WriteHashMap(cursor));

                // exercise the same data types, cycles, sharing, and reopening checks
                // as the in-memory offsets map, reusing the scratch map between runs.
                // reusing it must discard offsets from previous compactions.
                offsetMap.put(0, 123);
                testCompaction(sourceCore, targetCore, hasher, sourceFile, targetFile, offsetMap);
                assertNull(offsetMap.get(0));
            });
        }
    }

    private Database compact(Database source, Core targetCore, OffsetMap offsetMap) throws Exception {
        return offsetMap == null ? source.compact(targetCore) : source.compact(targetCore, offsetMap);
    }

    void testCompaction(Core sourceCore, Core targetCore, Hasher hasher, File sourceFile, File targetFile, OffsetMap offsetMap) throws Exception {
        // empty DB compaction
        {
            sourceCore.setLength(0);
            targetCore.setLength(0);
            var source = new Database(sourceCore, hasher);
            var compacted = compact(source, targetCore, offsetMap);
            assertTrue(compacted.rootCursor().slot().empty());
        }

        // basic compaction with various data types
        {
            sourceCore.setLength(0);
            targetCore.setLength(0);
            var source = new Database(sourceCore, hasher);

            // moment 1
            {
                var history = new WriteArrayList(source.rootCursor());
                history.appendContext(history.getSlot(-1), (cursor) -> {
                    var moment = new WriteHashMap(cursor);
                    moment.put("key1", new Database.Bytes("value1"));
                    moment.put("key2", new Database.Uint(100));
                });
            }

            // moment 2
            {
                var history = new WriteArrayList(source.rootCursor());
                history.appendContext(history.getSlot(-1), (cursor) -> {
                    var moment = new WriteHashMap(cursor);
                    moment.put("key1", new Database.Bytes("updated_value1"));
                    moment.put("key2", new Database.Uint(200));
                    moment.put("key3", new Database.Int(-42));
                    moment.put("key4", new Database.Float(3.14));
                    moment.put("short", new Database.Bytes("hi"));

                    // long bytes with format_tag
                    moment.put("tagged", new Database.Bytes("this is a long tagged string!!", "bi"));

                    // ArrayList
                    var fruitsCursor = moment.putCursor("fruits");
                    var fruits = new WriteArrayList(fruitsCursor);
                    fruits.append(new Database.Bytes("apple"));
                    fruits.append(new Database.Bytes("banana"));
                    fruits.append(new Database.Bytes("cherry"));

                    // LinkedArrayList
                    var todosCursor = moment.putCursor("todos");
                    var todos = new WriteLinkedArrayList(todosCursor);
                    todos.append(new Database.Bytes("task1"));
                    todos.append(new Database.Bytes("task2"));
                    todos.append(new Database.Bytes("task3"));

                    // CountedHashMap
                    var countedCursor = moment.putCursor("counted");
                    var counted = new WriteCountedHashMap(countedCursor);
                    counted.put("a", new Database.Uint(1));
                    counted.putKey("a", new Database.Bytes("a"));
                    counted.put("b", new Database.Uint(2));
                    counted.putKey("b", new Database.Bytes("b"));

                    // HashSet
                    var setCursor = moment.putCursor("myset");
                    var set = new WriteHashSet(setCursor);
                    set.put("x");
                    set.put("y");

                    // CountedHashSet
                    var csetCursor = moment.putCursor("mycset");
                    var cset = new WriteCountedHashSet(csetCursor);
                    cset.put("p");
                    cset.put("q");

                    // SortedMap
                    var sorted = new WriteSortedMap(moment.putCursor("sorted"));
                    sorted.put("apple", new Database.Uint(1));
                    sorted.put("banana", new Database.Uint(2));
                    sorted.put("cherry", new Database.Uint(3));

                    // SortedSet
                    var sortedSet = new WriteSortedSet(moment.putCursor("sortedset"));
                    sortedSet.put("foo");
                    sortedSet.put("bar");
                });
            }

            // moment 3
            {
                var history = new WriteArrayList(source.rootCursor());
                history.appendContext(history.getSlot(-1), (cursor) -> {
                    var moment = new WriteHashMap(cursor);
                    moment.put("key1", new Database.Bytes("final_value"));

                    // cycles must survive compaction rather than causing the
                    // remapper to recurse indefinitely.
                    moment.put("self", moment.slot());

                    var cyclicList = new WriteArrayList(moment.putCursor("cyclic-list"));
                    cyclicList.append(cyclicList.slot());

                    var mapA = new WriteHashMap(moment.putCursor("map-a"));
                    var mapB = new WriteHashMap(moment.putCursor("map-b"));
                    mapA.put("map-b", mapB.slot());
                    mapB.put("map-a", mapA.slot());
                });
            }

            var sourceSize = sourceCore.length();

            // compact
            var compacted = compact(source, targetCore, offsetMap);

            var targetSize = targetCore.length();

            // target should be smaller than source (3 moments vs 1)
            assertTrue(targetSize < sourceSize);

            // target should have exactly 1 moment
            var history = new ReadArrayList(compacted.rootCursor());
            assertEquals(1, history.count());

            // verify all data from latest moment is correct
            var momentCursor = history.getCursor(0);
            var moment = new ReadHashMap(momentCursor);

            // self-references and mutual references point back to the same compacted
            // objects rather than duplicate objects or dangling source offsets.
            var selfCursor = moment.getCursor("self");
            assertEquals(momentCursor.slot(), selfCursor.slot());
            assertEquals(momentCursor.slot().valueOffset().longValue(), selfCursor.slot().valueOffset().longValue());

            var cyclicListCursor = moment.getCursor("cyclic-list");
            var cyclicList = new ReadArrayList(cyclicListCursor);
            assertEquals(cyclicListCursor.slot(), cyclicList.getSlot(0));
            assertEquals(cyclicListCursor.slot().valueOffset().longValue(), cyclicList.getSlot(0).valueOffset().longValue());

            var mapACursor = moment.getCursor("map-a");
            var mapA = new ReadHashMap(mapACursor);
            var mapBCursor = mapA.getCursor("map-b");
            var mapB = new ReadHashMap(mapBCursor);
            assertEquals(mapACursor.slot(), mapB.getSlot("map-a"));
            assertEquals(mapACursor.slot().valueOffset().longValue(), mapB.getSlot("map-a").valueOffset().longValue());

            // key1 should have the final value
            assertEquals("final_value", new String(moment.getCursor("key1").readBytes(MAX_READ_BYTES)));

            // key2 from moment 2
            assertEquals(200, moment.getCursor("key2").readUint());

            // key3 - int
            assertEquals(-42, moment.getCursor("key3").readInt());

            // key4 - float
            assertEquals(3.14, moment.getCursor("key4").readFloat());

            // short bytes
            assertEquals("hi", new String(moment.getCursor("short").readBytes(MAX_READ_BYTES)));

            // tagged bytes
            var taggedObj = moment.getCursor("tagged").readBytesObject(MAX_READ_BYTES);
            assertEquals("this is a long tagged string!!", new String(taggedObj.value()));
            assertEquals("bi", new String(taggedObj.formatTag()));

            // ArrayList
            var fruitsCursor = moment.getCursor("fruits");
            var fruits = new ReadArrayList(fruitsCursor);
            assertEquals(3, fruits.count());
            assertEquals("apple", new String(fruits.getCursor(0).readBytes(MAX_READ_BYTES)));
            assertEquals("cherry", new String(fruits.getCursor(2).readBytes(MAX_READ_BYTES)));

            // LinkedArrayList
            var todosCursor = moment.getCursor("todos");
            var todos = new ReadLinkedArrayList(todosCursor);
            assertEquals(3, todos.count());
            assertEquals("task1", new String(todos.getCursor(0).readBytes(MAX_READ_BYTES)));
            assertEquals("task3", new String(todos.getCursor(2).readBytes(MAX_READ_BYTES)));

            // CountedHashMap
            var countedCursor = moment.getCursor("counted");
            var counted = new ReadCountedHashMap(countedCursor);
            assertEquals(2, counted.count());
            assertEquals(1, counted.getCursor("a").readUint());
            assertEquals(2, counted.getCursor("b").readUint());

            // HashSet
            var setCursor = moment.getCursor("myset");
            var set = new ReadHashSet(setCursor);
            assertEquals("x", new String(set.getCursor("x").readBytes(MAX_READ_BYTES)));

            // CountedHashSet
            var csetCursor = moment.getCursor("mycset");
            var cset = new ReadCountedHashSet(csetCursor);
            assertEquals(2, cset.count());
            assertEquals("p", new String(cset.getCursor("p").readBytes(MAX_READ_BYTES)));

            // SortedMap
            var sorted = new ReadSortedMap(moment.getCursor("sorted"));
            assertEquals(3, sorted.count());
            assertEquals(2, sorted.getCursor("banana").readUint());
            // lexicographic order is preserved across compaction
            assertEquals("apple", new String(sorted.getIndexKeyValuePair(0).keyCursor.readBytes(MAX_READ_BYTES)));
            assertEquals("cherry", new String(sorted.getIndexKeyValuePair(-1).keyCursor.readBytes(MAX_READ_BYTES)));

            // SortedSet
            var sortedSet = new ReadSortedSet(moment.getCursor("sortedset"));
            assertEquals(2, sortedSet.count());
            assertTrue(sortedSet.contains("foo"));
            assertTrue(sortedSet.contains("bar"));
            assertTrue(!sortedSet.contains("baz"));
        }

        // structural sharing (most data shared, only 1 key changes per moment)
        {
            sourceCore.setLength(0);
            targetCore.setLength(0);
            var source = new Database(sourceCore, hasher);

            // moment 1: create many keys
            {
                var history = new WriteArrayList(source.rootCursor());
                history.appendContext(history.getSlot(-1), (cursor) -> {
                    var moment = new WriteHashMap(cursor);
                    for (int i = 0; i < 20; i++) {
                        moment.put("shared_key_" + i, new Database.Uint(i));
                    }
                });
            }

            // moments 2-5: change only one key each time
            for (int round = 0; round < 4; round++) {
                var history = new WriteArrayList(source.rootCursor());
                final int r = round;
                history.appendContext(history.getSlot(-1), (cursor) -> {
                    var moment = new WriteHashMap(cursor);
                    moment.put("changing_key", new Database.Uint(r + 100));
                });
            }

            var compacted = compact(source, targetCore, offsetMap);

            var history = new ReadArrayList(compacted.rootCursor());
            assertEquals(1, history.count());

            var momentCursor = history.getCursor(0);
            var moment = new ReadHashMap(momentCursor);

            // verify shared keys are intact
            for (int i = 0; i < 20; i++) {
                assertEquals(i, moment.getCursor("shared_key_" + i).readUint());
            }

            // verify changing key has latest value
            assertEquals(103, moment.getCursor("changing_key").readUint());
        }

        // re-open after compact and compact-then-continue-writing
        // (only meaningful for file modes)
        if (sourceFile != null && targetFile != null) {
            // re-open after compact
            {
                sourceCore.setLength(0);
                targetCore.setLength(0);
                var source = new Database(sourceCore, hasher);

                // write some data
                {
                    var history = new WriteArrayList(source.rootCursor());
                    history.appendContext(history.getSlot(-1), (cursor) -> {
                        var moment = new WriteHashMap(cursor);
                        moment.put("persist", new Database.Bytes("persistent_value"));
                        moment.put("number", new Database.Uint(999));
                    });
                }

                // compact
                compact(source, targetCore, offsetMap);

                // re-open the target
                targetCore.seek(0);
                var reopened = new Database(targetCore, hasher);

                var history = new ReadArrayList(reopened.rootCursor());
                assertEquals(1, history.count());

                var momentCursor = history.getCursor(0);
                var moment = new ReadHashMap(momentCursor);
                assertEquals("persistent_value", new String(moment.getCursor("persist").readBytes(MAX_READ_BYTES)));
                assertEquals(999, moment.getCursor("number").readUint());
            }

            // compact then continue writing
            {
                sourceCore.setLength(0);
                targetCore.setLength(0);
                var source = new Database(sourceCore, hasher);

                // write initial data
                {
                    var history = new WriteArrayList(source.rootCursor());
                    history.appendContext(history.getSlot(-1), (cursor) -> {
                        var moment = new WriteHashMap(cursor);
                        moment.put("original", new Database.Bytes("original_data"));
                    });
                }

                // compact
                var compacted = compact(source, targetCore, offsetMap);

                // add new moment to compacted DB
                {
                    var history = new WriteArrayList(compacted.rootCursor());
                    history.appendContext(history.getSlot(-1), (cursor) -> {
                        var moment = new WriteHashMap(cursor);
                        moment.put("new_key", new Database.Bytes("new_data"));
                    });
                }

                // verify both old and new data
                var history = new ReadArrayList(compacted.rootCursor());
                assertEquals(2, history.count());

                // moment 0 (compacted original)
                var m0Cursor = history.getCursor(0);
                var m0 = new ReadHashMap(m0Cursor);
                assertEquals("original_data", new String(m0.getCursor("original").readBytes(MAX_READ_BYTES)));

                // moment 1 (new data added after compact)
                var m1Cursor = history.getCursor(1);
                var m1 = new ReadHashMap(m1Cursor);
                assertEquals("new_data", new String(m1.getCursor("new_key").readBytes(MAX_READ_BYTES)));

                // original data should still be in moment 1 (inherited)
                assertEquals("original_data", new String(m1.getCursor("original").readBytes(MAX_READ_BYTES)));
            }
        }
    }

    @Test
    void testSortedMap() throws Exception {
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            testSortedMap(core, hasher);
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();
            try (var core = new CoreFile(new RandomAccessFile(file, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testSortedMap(core, hasher);
            }
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();
            try (var core = new CoreBufferedFile(new RandomAccessBufferedFile(file, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testSortedMap(core, hasher);
            }
        }
    }

    void testSortedMap(Core core, Hasher hasher) throws Exception {
        var db = new Database(core, hasher);
        var rootCursor = db.rootCursor();

        // keys "k0000".."k0059" sort lexicographically in numeric order
        final int COUNT = 60;

        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                var moment = new WriteHashMap(cursor);
                var map = new WriteSortedMap(moment.putCursor("map"));

                // insert in reverse order to exercise front-insertions and splits
                for (int i = COUNT; i > 0;) {
                    i -= 1;
                    map.put(String.format("k%04d", i), new Database.Uint(i));
                }
                assertEquals(COUNT, map.count());

                // dedup: re-putting an existing key replaces the value, not the count
                map.put("k0005", new Database.Uint(999));
                assertEquals(COUNT, map.count());
                assertEquals(999, map.getCursor("k0005").readUint());
                map.put("k0005", new Database.Uint(5));

                // ordered iteration yields k0000..k0059 in order with intact values
                {
                    int n = 0;
                    var iter = map.iterator();
                    while (iter.hasNext()) {
                        var kv = iter.next().readKeyValuePair();
                        assertEquals(String.format("k%04d", n), new String(kv.keyCursor.readBytes(MAX_READ_BYTES)));
                        assertEquals(n, kv.valueCursor.readUint());
                        n += 1;
                    }
                    assertEquals(COUNT, n);
                }

                assertTrue(map.getCursor("k0042") != null);
                assertTrue(map.getCursor("nope") == null);

                // getByIndex (positive and negative) and rank are inverses
                {
                    for (int idx = 0; idx < COUNT; idx++) {
                        var kv = map.getIndexKeyValuePair(idx);
                        var key = new String(kv.keyCursor.readBytes(MAX_READ_BYTES));
                        assertEquals(String.format("k%04d", idx), key);
                        assertEquals(idx, map.rank(key));
                    }
                    var last = map.getIndexKeyValuePair(-1);
                    assertEquals("k0059", new String(last.keyCursor.readBytes(MAX_READ_BYTES)));
                    assertEquals(null, map.getIndexKeyValuePair(COUNT));
                }

                // lower-bound iteration from a present and an absent key
                {
                    var iter = map.iteratorFrom("k0030");
                    assertEquals("k0030", new String(iter.next().readKeyValuePair().keyCursor.readBytes(MAX_READ_BYTES)));
                }
                {
                    // "k00095" sorts between "k0009" and "k0010"
                    var iter = map.iteratorFrom("k00095");
                    assertEquals("k0010", new String(iter.next().readKeyValuePair().keyCursor.readBytes(MAX_READ_BYTES)));
                }
                {
                    var iter = map.iteratorFromIndex(COUNT - 2);
                    assertEquals("k0058", new String(iter.next().readKeyValuePair().keyCursor.readBytes(MAX_READ_BYTES)));
                }
                // negative indexes count from the end: -1 is the last entry, -COUNT the first
                {
                    var iter = map.iteratorFromIndex(-1);
                    assertEquals("k0059", new String(iter.next().readKeyValuePair().keyCursor.readBytes(MAX_READ_BYTES)));
                    assertTrue(!iter.hasNext()); // -1 is the last entry, so nothing follows
                }
                {
                    var iter = map.iteratorFromIndex(-COUNT);
                    assertEquals("k0000", new String(iter.next().readKeyValuePair().keyCursor.readBytes(MAX_READ_BYTES)));
                }
                // out of range past either end yields nothing
                {
                    assertTrue(!map.iteratorFromIndex(COUNT).hasNext());
                    assertTrue(!map.iteratorFromIndex(-COUNT - 1).hasNext());
                }

                // remove the even keys, then re-verify order, count, and presence
                {
                    for (int j = 0; j < COUNT; j += 2) {
                        assertTrue(map.remove(String.format("k%04d", j)));
                    }
                    assertEquals(COUNT / 2, map.count());
                    assertTrue(!map.remove("k0000")); // already gone

                    int expectI = 1;
                    int seen = 0;
                    var iter = map.iterator();
                    while (iter.hasNext()) {
                        var kv = iter.next().readKeyValuePair();
                        assertEquals(String.format("k%04d", expectI), new String(kv.keyCursor.readBytes(MAX_READ_BYTES)));
                        expectI += 2;
                        seen += 1;
                    }
                    assertEquals(COUNT / 2, seen);
                }

                // iterating-from on an unwritten (none) map yields nothing, like iterator()
                {
                    var noneCursor = new ReadCursor(new SlotPointer(null, new Slot()), db);
                    var empty = new ReadSortedMap(noneCursor);
                    assertTrue(!empty.iteratorFrom("anything").hasNext());
                    assertTrue(!empty.iteratorFromIndex(0).hasNext());
                }

                // SortedSet with mixed short (inline) and long (external) keys
                var set = new WriteSortedSet(moment.putCursor("set"));
                set.put("short");
                set.put("a-much-longer-key-stored-externally");
                set.put("mid");
                set.put("short"); // dup is a no-op
                assertEquals(3, set.count());
                assertTrue(set.contains("mid"));
                assertTrue(!set.contains("nope"));
                {
                    var want = new String[]{ "a-much-longer-key-stored-externally", "mid", "short" };
                    int n = 0;
                    var iter = set.iterator();
                    while (iter.hasNext()) {
                        var kv = iter.next().readKeyValuePair();
                        assertEquals(want[n], new String(kv.keyCursor.readBytes(MAX_READ_BYTES)));
                        n += 1;
                    }
                    assertEquals(3, n);
                }
                assertTrue(set.remove("mid"));
                assertEquals(2, set.count());

                // immutability guards: positional access is read-only, and keys/entries
                // cannot be overwritten through the low-level path API
                assertThrows(Database.WriteNotAllowedException.class, () -> {
                    ((WriteCursor)map.cursor).writePath(new Database.PathPart[]{
                        new Database.SortedMapGetIndex(0)
                    });
                });
                assertThrows(Database.CursorNotWriteableException.class, () -> {
                    ((WriteCursor)map.cursor).writePath(new Database.PathPart[]{
                        new Database.SortedMapGet(new Database.SortedMapGetKey("k0001".getBytes())),
                        new Database.WriteData(new Database.Bytes("x"))
                    });
                });

                // a write that fails after the key is inserted (a missing key written
                // through the non-writeable key slot) must still leave the count
                // consistent with the tree, not inserted-but-uncounted
                var countBeforeFailedWrite = map.count();
                assertThrows(Database.CursorNotWriteableException.class, () -> {
                    ((WriteCursor)map.cursor).writePath(new Database.PathPart[]{
                        new Database.SortedMapGet(new Database.SortedMapGetKey("missing-key".getBytes())),
                        new Database.WriteData(new Database.Bytes("x"))
                    });
                });
                assertEquals(countBeforeFailedWrite + 1, map.count());
                assertTrue(map.getKeyValuePair("missing-key") != null);
                map.remove("missing-key"); // restore the map for the assertions below
            })
        });

        // the map persists in the committed moment
        {
            var history = new ReadArrayList(db.rootCursor());
            var moment = new ReadHashMap(history.getCursor(-1));
            var map = new ReadSortedMap(moment.getCursor("map"));
            assertEquals(COUNT / 2, map.count());
            assertEquals("k0001", new String(map.getIndexKeyValuePair(0).keyCursor.readBytes(MAX_READ_BYTES)));

            // entries have no hash
            var entry = map.cursor.readPathSlot(new Database.PathPart[]{
                new Database.SortedMapGet(new Database.SortedMapGetKVPair("k0001".getBytes()))
            });
            assertEquals(Tag.SORTED_KV_PAIR, entry.tag());
        }

        // a second moment that inherits and mutates the map must not disturb the first
        // (copy-on-write immutability across transactions)
        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                var moment = new WriteHashMap(cursor);
                var map = new WriteSortedMap(moment.putCursor("map"));
                assertTrue(map.remove("k0001"));
                map.put("k0001", new Database.Uint(7)); // not in moment 0
            })
        });
        {
            var history = new ReadArrayList(db.rootCursor());

            // moment 0 (original) is unchanged: k0001 still present with value 1
            var m0 = new ReadHashMap(history.getCursor(0));
            var map0 = new ReadSortedMap(m0.getCursor("map"));
            assertEquals(COUNT / 2, map0.count());
            assertEquals(1, map0.getCursor("k0001").readUint());

            // moment 1 reflects the mutation: k0001 re-added with value 7
            var m1 = new ReadHashMap(history.getCursor(1));
            var map1 = new ReadSortedMap(m1.getCursor("map"));
            assertEquals(COUNT / 2, map1.count());
            assertEquals(7, map1.getCursor("k0001").readUint());
        }
    }

    @Test
    void testIteratorFrom() throws Exception {
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            testIteratorFrom(core, hasher);
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();
            try (var core = new CoreFile(new RandomAccessFile(file, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testIteratorFrom(core, hasher);
            }
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();
            try (var core = new CoreBufferedFile(new RandomAccessBufferedFile(file, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testIteratorFrom(core, hasher);
            }
        }
    }

    void testIteratorFrom(Core core, Hasher hasher) throws Exception {
        var db = new Database(core, hasher);
        var rootCursor = db.rootCursor();

        // enough items to force several tiers in both the array-list radix trie
        // (16^2 = 256 > 200) and the linked-array-list b-tree
        final int COUNT = 200;

        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                var moment = new WriteHashMap(cursor);
                var list = new WriteArrayList(moment.putCursor("list"));
                for (int i = 0; i < COUNT; i++) list.append(new Database.Uint(i));
                var linked = new WriteLinkedArrayList(moment.putCursor("linked"));
                for (int i = 0; i < COUNT; i++) linked.append(new Database.Uint(i));

                // the write-side structs expose iteratorFrom too, yielding write cursors
                {
                    WriteCursor.Iterator it = list.iteratorFrom(COUNT - 3);
                    long expected = COUNT - 3;
                    while (it.hasNext()) {
                        assertEquals(expected, it.next().readUint());
                        expected += 1;
                    }
                    assertEquals(COUNT, expected);
                }
                {
                    WriteCursor.Iterator it = linked.iteratorFrom(-2);
                    long expected = COUNT - 2;
                    while (it.hasNext()) {
                        assertEquals(expected, it.next().readUint());
                        expected += 1;
                    }
                    assertEquals(COUNT, expected);
                }
            })
        });

        var history = new ReadArrayList(db.rootCursor());
        var moment = new ReadHashMap(history.getCursor(-1));
        var list = new ReadArrayList(moment.getCursor("list"));
        var linked = new ReadLinkedArrayList(moment.getCursor("linked"));

        // iteratorFrom(k) yields exactly k, k+1, .., COUNT-1 in order, for both
        // types. negative indexes count from the end: -1 starts at the last
        // element, -COUNT at the first.
        long[] starts = { 0, 1, 15, 16, 17, 100, COUNT - 2, COUNT - 1, -1, -2, -16, -100, -COUNT };
        for (long start : starts) {
            long resolved = start < 0 ? COUNT + start : start;
            {
                var iter = list.iteratorFrom(start);
                long expected = resolved;
                while (iter.hasNext()) {
                    assertEquals(expected, iter.next().readUint());
                    expected += 1;
                }
                assertEquals(COUNT, expected);
            }
            {
                var iter = linked.iteratorFrom(start);
                long expected = resolved;
                while (iter.hasNext()) {
                    assertEquals(expected, iter.next().readUint());
                    expected += 1;
                }
                assertEquals(COUNT, expected);
            }
        }

        // iteratorFrom(0) matches a plain iterator()
        {
            var a = list.iterator();
            var b = list.iteratorFrom(0);
            while (a.hasNext()) {
                assertEquals(a.next().readUint(), b.next().readUint());
            }
            assertTrue(!b.hasNext());
        }

        // a start out of range (past the end, or more negative than -COUNT) yields nothing
        long[] outOfRange = { COUNT, COUNT + 1, COUNT + 1000, -COUNT - 1, -COUNT - 1000 };
        for (long start : outOfRange) {
            assertTrue(!list.iteratorFrom(start).hasNext());
            assertTrue(!linked.iteratorFrom(start).hasNext());
        }
    }

    @Test
    void testReplication() throws Exception {
        try (var sourceCore = new CoreMemory(new RandomAccessMemory());
             var replicaCore = new CoreMemory(new RandomAccessMemory())) {
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            testReplication(sourceCore, replicaCore, hasher);
            testRoots(sourceCore, replicaCore, hasher);
        }

        {
            var sourceFile = File.createTempFile("database", "");
            sourceFile.deleteOnExit();
            var replicaFile = File.createTempFile("replica", "");
            replicaFile.deleteOnExit();

            try (var sourceCore = new CoreFile(new RandomAccessFile(sourceFile, "rw"));
                 var replicaCore = new CoreFile(new RandomAccessFile(replicaFile, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testReplication(sourceCore, replicaCore, hasher);
                testRoots(sourceCore, replicaCore, hasher);
            }
        }

        {
            var sourceFile = File.createTempFile("database", "");
            sourceFile.deleteOnExit();
            var replicaFile = File.createTempFile("replica", "");
            replicaFile.deleteOnExit();

            try (var sourceCore = new CoreBufferedFile(new RandomAccessBufferedFile(sourceFile, "rw"));
                 var replicaCore = new CoreBufferedFile(new RandomAccessBufferedFile(replicaFile, "rw"))) {
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testReplication(sourceCore, replicaCore, hasher);
                testRoots(sourceCore, replicaCore, hasher);
            }
        }

        testReplicaOfOtherFile();
    }

    // a replica is never extended with bytes from a file it isn't a replica of
    void testReplicaOfOtherFile() throws Exception {
        var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
        try (var sourceCore = new CoreMemory(new RandomAccessMemory());
             var otherCore = new CoreMemory(new RandomAccessMemory());
             var restoredCore = new CoreMemory(new RandomAccessMemory());
             var replicaCore = new CoreMemory(new RandomAccessMemory())) {
            var source = new Database(sourceCore, hasher, 1L);
            var other = new Database(otherCore, hasher, 2L);
            var replica = new Database(replicaCore, hasher);

            // the same writes give both files the same layout
            var sourceRoot = source.rootCursor();
            sourceRoot.write(new Database.Uint(1));
            var otherRoot = other.rootCursor();
            otherRoot.write(new Database.Uint(1));
            replicate(source, replica);

            // the other file has a trailer where the replica's committed bytes end,
            // but its checksum covers a different file ID, so the replica is replaced
            otherRoot.write(new Database.Uint(2));
            assertEquals(0, other.replicationStart(replica.replicaState()));

            // and if its bytes are appended anyway, they are rejected
            {
                var start = replica.committedSize();
                var end = other.committedSize();
                var buffer = new ByteArrayOutputStream();
                other.readCommitted(start, end, buffer);
                var reader = new ByteArrayInputStream(buffer.toByteArray());
                assertThrows(Database.InvalidDatabaseException.class, () -> replica.applyReplicated(start, reader, end - start));
                assertEquals(start, replica.committedSize());
            }

            // a source restored from an older copy keeps its file ID, but
            // its history diverges, so the replica is replaced too
            var restored = new Database(restoredCore, hasher, 1L);
            var restoredRoot = restored.rootCursor();
            restoredRoot.write(new Database.Uint(3));
            assertEquals(0, restored.replicationStart(replica.replicaState()));

            // copies of one file that write different data of the same size have the
            // same layout, but every commit has a random ID, so their histories differ
            try (var copyACore = new CoreMemory(new RandomAccessMemory());
                 var copyBCore = new CoreMemory(new RandomAccessMemory());
                 var copyReplicaCore = new CoreMemory(new RandomAccessMemory())) {
                var copyA = new Database(copyACore, hasher, 3L);
                var copyB = new Database(copyBCore, hasher, 3L);
                var copyReplica = new Database(copyReplicaCore, hasher);
                copyA.rootCursor().write(new Database.Bytes("aaaaaaaaaaaa"));
                copyB.rootCursor().write(new Database.Bytes("bbbbbbbbbbbb"));

                replicate(copyA, copyReplica);
                assertEquals(copyA.committedSize(), copyB.committedSize());
                assertEquals(0, copyB.replicationStart(copyReplica.replicaState()));
                replicate(copyB, copyReplica);
                assertEquals("bbbbbbbbbbbb", new String(copyReplica.rootCursor().readBytes(MAX_READ_BYTES)));
            }

            // while the real source only sends the new bytes
            sourceRoot.write(new Database.Uint(2));
            assertEquals(replica.committedSize(), source.replicationStart(replica.replicaState()));
            replicate(source, replica);
            expectSameCommittedBytes(source, replica);
        }
    }

    static byte[] readCommittedBytes(Database db, long start) throws Exception {
        var buffer = new ByteArrayOutputStream();
        db.readCommitted(start, db.committedSize(), buffer);
        return buffer.toByteArray();
    }

    // the replica sends its state, and the source sends back the bytes it doesn't
    // have yet (or all of them, if it isn't a replica of this source)
    static void replicate(Database source, Database replica) throws Exception {
        var start = source.replicationStart(replica.replicaState());
        var end = source.committedSize();
        var buffer = new ByteArrayOutputStream();
        source.readCommitted(start, end, buffer);
        var reader = new ByteArrayInputStream(buffer.toByteArray());
        replica.applyReplicated(start, reader, end - start);
    }

    // checks that no committed byte has changed since `committed` was read,
    // except the root header (the root slot and file size after the database
    // header). then replicates and checks that the replica is identical.
    // returns the bytes that are committed now.
    static byte[] expectAppendOnlyAndReplicate(Database source, Database replica, byte[] committed) throws Exception {
        var rootHeaderStart = Database.Header.length;
        var rootHeaderEnd = rootHeaderStart + 17;
        var current = readCommittedBytes(source, 0);
        assertTrue(current.length >= committed.length);
        assertArrayEquals(Arrays.copyOfRange(committed, 0, rootHeaderStart), Arrays.copyOfRange(current, 0, rootHeaderStart));
        assertArrayEquals(Arrays.copyOfRange(committed, rootHeaderEnd, committed.length), Arrays.copyOfRange(current, rootHeaderEnd, committed.length));

        replicate(source, replica);
        var replicaBytes = readCommittedBytes(replica, 0);
        assertArrayEquals(current, replicaBytes);
        return current;
    }

    static void expectSameCommittedBytes(Database source, Database replica) throws Exception {
        var sourceBytes = readCommittedBytes(source, 0);
        var replicaBytes = readCommittedBytes(replica, 0);
        assertArrayEquals(sourceBytes, replicaBytes);
    }

    static class FailException extends Exception {}

    // writes moment i: a key, and an item appended to a list. if fail is true, it then throws.
    static Database.ContextFunction momentContext(long i, boolean fail) {
        return (cursor) -> {
            var moment = new WriteHashMap(cursor);
            moment.put("key-" + i, new Database.Uint(i));
            var list = new WriteArrayList(moment.putCursor("list"));
            list.append(new Database.Uint(i));
            if (fail) throw new FailException();
        };
    }

    void testReplication(Core sourceCore, Core replicaCore, Hasher hasher) throws Exception {
        sourceCore.setLength(0);
        replicaCore.setLength(0);

        // a fixed file ID, so the bytes of the new database are known
        var source = new Database(sourceCore, hasher, 0x0102030405060708L);
        var replica = new Database(replicaCore, hasher);

        // a new database is its header (which ends with its file ID), its root
        // header, and the trailer for its empty root: the slot (9 zero bytes), the commit ID (0 for
        // the first commit), then the CRC-64/XZ of the file ID, the trailer's position
        // (37), the slot, and the commit ID
        {
            var bytes = readCommittedBytes(source, 0);
            assertEquals(62, bytes.length);
            assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, Arrays.copyOfRange(bytes, 12, 20));
            assertArrayEquals(new byte[]{
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, (byte) 0xd6, 0x18, 0x4e, 0x46, (byte) 0xb4, 0x7d, 0x29,
                0x26,
            }, Arrays.copyOfRange(bytes, 37, bytes.length));
        }

        // new databases have their own random file IDs, so the
        // first sync replaces the replica with a copy of the source
        assertEquals(0, source.replicationStart(replica.replicaState()));
        replicate(source, replica);
        expectSameCommittedBytes(source, replica);

        assertEquals(Database.VERSION, source.header.version());

        var history = new WriteArrayList(source.rootCursor());

        var committed = readCommittedBytes(source, 0);

        // enough moments for the top-level array list to grow to three levels
        for (int i = 0; i < 300; i++) {
            if (i == 200) {
                // roll back the history
                history.slice(150);
            } else if (i % 50 == 25) {
                // a failed transaction
                final int n = i;
                assertThrows(FailException.class, () -> history.appendContext(history.getSlot(-1), momentContext(n, true)));
            } else {
                history.appendContext(history.getSlot(-1), momentContext(i, false));
            }

            committed = expectAppendOnlyAndReplicate(source, replica, committed);
        }

        // applyReplicated must be given exactly the bytes up to a trailer.
        // a short append fails and is overwritten by the next one.
        {
            history.appendContext(history.getSlot(-1), momentContext(300, false));
            var start = replica.committedSize();
            var end = source.committedSize();
            var bytes = readCommittedBytes(source, start);
            var shortReader = new ByteArrayInputStream(bytes, 0, bytes.length - 1);
            assertThrows(Database.InvalidDatabaseException.class, () -> replica.applyReplicated(start, shortReader, bytes.length - 1));
            assertEquals(start, replica.committedSize());
            // and only committed bytes can be read, starting at 0 or after the root header
            var discarding = OutputStream.nullOutputStream();
            assertThrows(Database.InvalidRangeException.class, () -> source.readCommitted(start, end + 1, discarding));
            assertThrows(Database.InvalidRangeException.class, () -> source.readCommitted(20, end, discarding));
            replicate(source, replica);
            expectSameCommittedBytes(source, replica);
        }

        // openCommitted and openReplicated stream the same bytes in pieces.
        // a stream from 0 substitutes the root header, so read across it too.
        {
            history.appendContext(history.getSlot(-1), momentContext(301, false));
            var start = replica.committedSize();
            var end = source.committedSize();
            for (var from : new long[] {0, start}) {
                var expected = new ByteArrayOutputStream();
                source.readCommitted(from, end, expected);
                var actual = new ByteArrayOutputStream();
                try (var reader = source.openCommitted(from, end)) {
                    var buffer = new byte[7];
                    int n;
                    while ((n = reader.read(buffer, 0, buffer.length)) != -1) actual.write(buffer, 0, n);
                }
                assertArrayEquals(expected.toByteArray(), actual.toByteArray());
            }

            var bytes = readCommittedBytes(source, start);
            // closing the stream early leaves the replica at its last commit
            var early = replica.openReplicated(start, bytes.length);
            early.write(bytes, 0, 10);
            assertThrows(Database.EndOfStreamException.class, early::close);
            assertEquals(start, replica.committedSize());
            // and it can't take more bytes than it was asked for
            try (var writer = replica.openReplicated(start, bytes.length)) {
                for (var i = 0; i < bytes.length; i += 7) writer.write(bytes, i, Math.min(7, bytes.length - i));
                assertThrows(Database.InvalidRangeException.class, () -> writer.write(0));
            }
            expectSameCommittedBytes(source, replica);
        }

        // the replica can be reopened and read
        var reopenedReplica = new Database(replicaCore, hasher);
        var sourceHistory = new ReadArrayList(source.rootCursor());
        var replicaHistory = new ReadArrayList(reopenedReplica.rootCursor());
        assertEquals(sourceHistory.count(), replicaHistory.count());
        var moment = new ReadHashMap(replicaHistory.getCursor(-1));
        assertEquals(299, moment.getCursor("key-299").readUint());
        // removed by the slice
        assertNull(moment.getCursor("key-180"));
        assertEquals(149, moment.getCursor("key-149").readUint());

        // a compacted database is a new file, so a replica of it is seeded from byte 0
        try (var targetCore = new CoreMemory(new RandomAccessMemory());
             var compactedReplicaCore = new CoreMemory(new RandomAccessMemory())) {
            var target = source.compact(targetCore);
            var compactedReplica = new Database(compactedReplicaCore, hasher);
            replicate(target, compactedReplica);
            expectSameCommittedBytes(target, compactedReplica);

            // and it can keep receiving transactions
            var targetHistory = new WriteArrayList(target.rootCursor());
            targetHistory.append(targetHistory.getSlot(-1));
            replicate(target, compactedReplica);
            expectSameCommittedBytes(target, compactedReplica);

            var compactedHistory = new ReadArrayList(compactedReplica.rootCursor());
            assertEquals(2, compactedHistory.count());
            var compactedMoment = new ReadHashMap(compactedHistory.getCursor(-1));
            assertEquals(299, compactedMoment.getCursor("key-299").readUint());
        }

        // a database whose trailer doesn't match its header is invalid
        {
            var pos = reopenedReplica.committedSize() - 1;
            reopenedReplica.core.seek(pos);
            var b = reopenedReplica.core.reader().readByte();
            reopenedReplica.core.seek(pos);
            reopenedReplica.core.writer().writeByte(~b);
            reopenedReplica.core.flush();
            assertThrows(Database.InvalidDatabaseException.class, () -> new Database(replicaCore, hasher));
        }
    }

    // the root can be any type, not just an array list
    void testRoots(Core sourceCore, Core replicaCore, Hasher hasher) throws Exception {
        // a hash map root
        {
            sourceCore.setLength(0);
            replicaCore.setLength(0);

            var source = new Database(sourceCore, hasher);
            var replica = new Database(replicaCore, hasher);

            var map = new WriteHashMap(source.rootCursor());

            // initializing an initialized root doesn't start a transaction
            var sizeAfterInit = source.committedSize();
            new WriteHashMap(source.rootCursor());
            assertEquals(sizeAfterInit, source.committedSize());

            // nor can the root be initialized as another type
            assertThrows(Database.UnexpectedTagException.class, () -> new WriteArrayList(source.rootCursor()));
            assertEquals(sizeAfterInit, source.committedSize());

            var committed = readCommittedBytes(source, 0);

            for (int i = 0; i < 100; i++) {
                map.put("key-" + i, new Database.Uint(i));
                if (i == 50) {
                    assertTrue(map.remove("key-25"));
                }

                committed = expectAppendOnlyAndReplicate(source, replica, committed);
            }

            // a key is written in the same transaction as the lookup, so it works on a root map.
            // (the string-key puts above already wrote their keys, so use an entry without one.)
            map.put(source.hash("key-only-hashed".getBytes()), new Database.Uint(1));
            map.putKey("key-only-hashed", new Database.Bytes("key-only-hashed"));
            {
                var kv = map.getKeyValuePair("key-only-hashed");
                assertEquals("key-only-hashed", new String(kv.keyCursor.readBytes(MAX_READ_BYTES)));
                assertEquals(1, kv.valueCursor.readUint());
            }

            // nested collections are written inside one transaction
            ((WriteCursor) map.cursor).writePath(new Database.PathPart[]{
                new Database.HashMapGet(new Database.HashMapGetValue(source.hash("inner".getBytes()))),
                new Database.Context((cursor) -> {
                    var inner = new WriteHashMap(cursor);
                    inner.put("foo", new Database.Bytes("foo"));
                    var list = new WriteArrayList(inner.putCursor("list"));
                    list.append(new Database.Uint(1));
                    list.append(new Database.Uint(2));
                })
            });
            replicate(source, replica);
            expectSameCommittedBytes(source, replica);

            // runContext groups many writes into one transaction
            {
                var sizeBefore = source.committedSize();
                Database.ContextFunction batch = (cursor) -> {
                    var root = new WriteHashMap(cursor);
                    for (int i = 0; i < 100; i++) {
                        root.put("batch-" + i, new Database.Uint(i));
                    }
                };

                // a failed transaction leaves nothing behind
                assertThrows(FailException.class, () -> source.runContext((cursor) -> {
                    batch.run(cursor);
                    throw new FailException();
                }));
                assertEquals(sizeBefore, source.committedSize());
                assertNull(map.getCursor("batch-0"));

                // a successful one commits everything at once
                source.runContext(batch);
                assertTrue(source.committedSize() > sizeBefore);
                assertEquals(0, map.getCursor("batch-0").readUint());
                assertEquals(99, map.getCursor("batch-99").readUint());
                replicate(source, replica);
                expectSameCommittedBytes(source, replica);
            }

            // a nested write cursor can't be written to after its transaction
            var lateCursor = map.putCursor("late");
            assertThrows(IllegalStateException.class, () -> lateCursor.write(new Database.Uint(1)));
            assertThrows(IllegalStateException.class, () -> new WriteHashMap(lateCursor));
            replicate(source, replica);
            expectSameCommittedBytes(source, replica);

            // the replica can be reopened and read
            replica = new Database(replicaCore, hasher);
            {
                var replicaMap = new ReadHashMap(replica.rootCursor());
                assertEquals(99, replicaMap.getCursor("key-99").readUint());
                assertNull(replicaMap.getCursor("key-25"));
                var inner = new ReadHashMap(replicaMap.getCursor("inner"));
                var list = new ReadArrayList(inner.getCursor("list"));
                assertEquals(2, list.count());
                assertEquals(2, list.getCursor(-1).readUint());
            }

            // it can be compacted
            try (var targetCore = new CoreMemory(new RandomAccessMemory());
                 var compactedReplicaCore = new CoreMemory(new RandomAccessMemory())) {
                var target = replica.compact(targetCore);
                assertTrue(target.committedSize() < replica.committedSize());

                var targetMap = new ReadHashMap(target.rootCursor());
                assertEquals(99, targetMap.getCursor("key-99").readUint());
                assertNull(targetMap.getCursor("key-25"));
                var inner = new ReadHashMap(targetMap.getCursor("inner"));
                var list = new ReadArrayList(inner.getCursor("list"));
                assertEquals(2, list.count());

                // a compacted database is a new file, so a replica of it is seeded from byte 0
                var compactedReplica = new Database(compactedReplicaCore, hasher);
                replicate(target, compactedReplica);
                expectSameCommittedBytes(target, compactedReplica);
            }
        }

        // an array list root held by a wrapper stays current
        {
            sourceCore.setLength(0);
            var db = new Database(sourceCore, hasher);

            var history = new WriteArrayList(db.rootCursor());
            history.append(new Database.Uint(1));
            history.append(new Database.Uint(2));
            assertEquals(2, history.count());
            var iter = history.iterator();
            int iterCount = 0;
            while (iter.hasNext()) {
                iter.next();
                iterCount += 1;
            }
            assertEquals(2, iterCount);

            // initializing an initialized root doesn't start a transaction
            var size = db.committedSize();
            new WriteArrayList(db.rootCursor());
            assertEquals(size, db.committedSize());
        }

        // a scalar root
        {
            sourceCore.setLength(0);
            var db = new Database(sourceCore, hasher);

            var retained = db.rootCursor();
            var root = db.rootCursor();
            root.write(new Database.Uint(42));
            assertEquals(42, root.readUint());
            assertEquals(42, db.rootCursor().readUint());

            // a root cursor kept from before the write sees the new root
            // everywhere it reads its slot, not just in typed reads
            assertEquals(42, retained.readUint());
            assertEquals(42, retained.slot().value());
            assertEquals(42, retained.readPathSlot(new Database.PathPart[0]).value());
            assertEquals(42, retained.readPath(new Database.PathPart[0]).readUint());

            // the cursor stays a root cursor, so it can keep writing
            root.write(new Database.Bytes("this is too long to fit in a slot"));
            assertEquals("this is too long to fit in a slot", new String(db.rootCursor().readBytes(MAX_READ_BYTES)));

            // bytes can't be streamed into the root
            assertThrows(Database.CursorNotWriteableException.class, () -> root.writer());

            // a scalar root can't be initialized as a collection
            assertThrows(Database.UnexpectedTagException.class, () -> new WriteHashMap(db.rootCursor()));

            // but once it is cleared, it can
            root.write(null);
            var map = new WriteHashMap(db.rootCursor());
            map.put("foo", new Database.Uint(1));
            assertEquals(1, map.getCursor("foo").readUint());
        }

        // a hash set root
        {
            sourceCore.setLength(0);
            var db = new Database(sourceCore, hasher);

            var set = new WriteHashSet(db.rootCursor());
            set.put("a");
            set.put("a");
            set.put("b");
            assertEquals("a", new String(set.getCursor("a").readBytes(MAX_READ_BYTES)));
            assertEquals("b", new String(set.getCursor("b").readBytes(MAX_READ_BYTES)));
        }

        // a sorted map root
        {
            sourceCore.setLength(0);
            var db = new Database(sourceCore, hasher);

            var map = new WriteSortedMap(db.rootCursor());
            map.put("b", new Database.Uint(2));
            map.put("a", new Database.Uint(1));
            map.put("c", new Database.Uint(3));
            assertTrue(map.remove("c"));
            assertEquals(2, map.count());
            assertEquals(1, map.rank("b"));
            assertEquals(1, map.getCursor("a").readUint());

            var size = db.committedSize();
            new WriteSortedMap(db.rootCursor());
            assertEquals(size, db.committedSize());
        }

        // a linked array list root
        {
            sourceCore.setLength(0);
            var db = new Database(sourceCore, hasher);

            var list = new WriteLinkedArrayList(db.rootCursor());
            list.append(new Database.Uint(1));
            list.append(new Database.Uint(3));
            list.insert(1, new Database.Uint(2));
            assertEquals(3, list.count());
            assertEquals(2, list.getCursor(1).readUint());
            list.remove(0);
            assertEquals(2, list.getCursor(0).readUint());

            var size = db.committedSize();
            new WriteLinkedArrayList(db.rootCursor());
            assertEquals(size, db.committedSize());
        }
    }
}
