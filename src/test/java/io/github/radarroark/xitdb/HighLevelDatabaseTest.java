package io.github.radarroark.xitdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.junit.jupiter.api.Test;

class HighLevelDatabaseTest {
    static long MAX_READ_BYTES = 1024;

    @Test
    void testHightLevelApi() throws Exception {
        try (var ram = new RandomAccessMemory()) {
            var core = new CoreMemory(ram);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            testHighLevelApi(core, hasher, null);
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();

            try (var raf = new RandomAccessFile(file, "rw")) {
                var core = new CoreFile(raf);
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testHighLevelApi(core, hasher, file);
            }
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();

            try (var raf = new RandomAccessBufferedFile(file, "rw")) {
                var core = new CoreBufferedFile(raf);
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
        try (var ram = new RandomAccessMemory()) {
            var core = new CoreMemory(ram);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            var db = new Database(core, hasher);

            var map = new WriteHashMap(db.rootCursor());
            map.put("foo", new Database.Bytes("foo"));
            map.put("bar", new Database.Bytes("bar"));

            // init inner map
            {
                var innerMapCursor = map.putCursor("inner-map");
                new WriteHashMap(innerMapCursor);
            }

            // re-init inner map
            // since the top-level type isn't an arraylist, there is no concept of
            // a transaction, so this does not perform a copy of the root node
            {
                var innerMapCursor = map.putCursor("inner-map");
                new WriteHashMap(innerMapCursor);
            }
        }

        // linked array list is not currently allowed at the top level
        try (var ram = new RandomAccessMemory()) {
            var core = new CoreMemory(ram);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            var db = new Database(core, hasher);

            assertThrows(Database.InvalidTopLevelTypeException.class, () -> new WriteLinkedArrayList(db.rootCursor()));
        }
    }

    @Test
    void testReadDatabaseFromResources() throws Exception {
        var resource = getClass().getClassLoader().getResource("test.db");
        File file = new File(resource.toURI());
        try (var raf = new RandomAccessFile(file, "r")) {
            var core = new CoreFile(raf);
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
                    var raf = new RandomAccessFile(file, "r");
                    var core = new CoreFile(raf);
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
                    ((CoreFile)db.get().core).file.close();
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
                    ((CoreFile)db.get().core).file.close();
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
        ((CoreFile)db.get().core).file.close();
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

        // the db size remains the same after writing junk data
        // and then reinitializing the db. this is useful because
        // there could be data from a transaction that never
        // completed due to an unclean shutdown.
        {
            core.seek(core.length());
            var sizeBefore = core.length();

            var writer = core.writer();
            writer.write("this is junk data that will be deleted during init".getBytes());

            // no error is thrown if db file is opened in read-only mode
            if (fileMaybe != null) {
                try (var raf = new RandomAccessFile(fileMaybe, "r")) {
                    new Database(new CoreFile(raf), hasher);
                }
            }

            db = new Database(core, hasher);

            var sizeAfter = core.length();

            assertEquals(sizeBefore, sizeAfter);
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

                // freeze here, so big-cities won't be mutated
                cursor.db.freeze();

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

            // and big-cities only contains the original two
            var bigCitiesCursor = moment.getCursor("big-cities");
            var bigCities = new ReadArrayList(bigCitiesCursor);
            assertEquals(2, bigCities.count());
        }
    }

    @Test
    void testCompaction() throws Exception {
        // memory
        try (var ram = new RandomAccessMemory()) {
            var core = new CoreMemory(ram);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            try (var targetRam = new RandomAccessMemory()) {
                var targetCore = new CoreMemory(targetRam);
                testCompaction(core, targetCore, hasher, null, null);
            }
        }

        // file
        {
            var sourceFile = File.createTempFile("compact_source", ".db");
            sourceFile.deleteOnExit();
            var targetFile = File.createTempFile("compact_target", ".db");
            targetFile.deleteOnExit();

            try (var sourceRaf = new RandomAccessFile(sourceFile, "rw");
                 var targetRaf = new RandomAccessFile(targetFile, "rw")) {
                var sourceCore = new CoreFile(sourceRaf);
                var targetCore = new CoreFile(targetRaf);
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testCompaction(sourceCore, targetCore, hasher, sourceFile, targetFile);
            }
        }

        // buffered file
        {
            var sourceFile = File.createTempFile("compact_source", ".db");
            sourceFile.deleteOnExit();
            var targetFile = File.createTempFile("compact_target", ".db");
            targetFile.deleteOnExit();

            try (var sourceRaf = new RandomAccessBufferedFile(sourceFile, "rw");
                 var targetRaf = new RandomAccessBufferedFile(targetFile, "rw")) {
                var sourceCore = new CoreBufferedFile(sourceRaf);
                var targetCore = new CoreBufferedFile(targetRaf);
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testCompaction(sourceCore, targetCore, hasher, sourceFile, targetFile);
            }
        }

        // buffered file to memory
        {
            var sourceFile = File.createTempFile("compact_source", ".db");
            sourceFile.deleteOnExit();

            try (var sourceRaf = new RandomAccessBufferedFile(sourceFile, "rw");
                 var targetRam = new RandomAccessMemory()) {
                var sourceCore = new CoreBufferedFile(sourceRaf);
                var targetCore = new CoreMemory(targetRam);
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testCompaction(sourceCore, targetCore, hasher, sourceFile, null);
            }
        }
    }

    void testCompaction(Core sourceCore, Core targetCore, Hasher hasher, File sourceFile, File targetFile) throws Exception {
        // empty DB compaction
        {
            sourceCore.setLength(0);
            targetCore.setLength(0);
            var source = new Database(sourceCore, hasher);
            var compacted = source.compact(targetCore);
            assertEquals(Tag.NONE, compacted.header.tag());
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
                });
            }

            // moment 3
            {
                var history = new WriteArrayList(source.rootCursor());
                history.appendContext(history.getSlot(-1), (cursor) -> {
                    var moment = new WriteHashMap(cursor);
                    moment.put("key1", new Database.Bytes("final_value"));
                });
            }

            var sourceSize = sourceCore.length();

            // compact
            var compacted = source.compact(targetCore);

            var targetSize = targetCore.length();

            // target should be smaller than source (3 moments vs 1)
            assertTrue(targetSize < sourceSize);

            // target should have exactly 1 moment
            var history = new ReadArrayList(compacted.rootCursor());
            assertEquals(1, history.count());

            // verify all data from latest moment is correct
            var momentCursor = history.getCursor(0);
            var moment = new ReadHashMap(momentCursor);

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

            var compacted = source.compact(targetCore);

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
                source.compact(targetCore);

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
                var compacted = source.compact(targetCore);

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
}
