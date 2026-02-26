<p align="center">
  xitdb is an immutable database written in Java
  <br/>
  <br/>
  <b>Choose your flavor:</b>
  <a href="https://github.com/xit-vcs/xitdb">Zig</a> |
  <a href="https://github.com/xit-vcs/xitdb-java">Java</a> |
  <a href="https://github.com/codeboost/xitdb-clj">Clojure</a> |
  <a href="https://github.com/xit-vcs/xitdb-ts">TypeScript</a>
</p>

* Each transaction efficiently creates a new "copy" of the database, and past copies can still be read from and reverted to.
* Supports storing in a single file as well as purely in-memory use.
* Runs as a library (embedded in process).
* Incrementally reads and writes, so file-based databases can contain larger-than-memory datasets.
* Reads never block writes, and a database can be read from multiple threads/processes without locks.
* No query engine of any kind. You just write data structures (primarily an `ArrayList` and `HashMap`) that can be nested arbitrarily.
* No dependencies besides the Java standard library (currently requires Java 17).
* Available [on Clojars](https://clojars.org/io.github.radarroark/xitdb).

This database was originally made for the [xit version control system](https://github.com/xit-vcs/xit), but I bet it has a lot of potential for other projects. The combination of being immutable and having an API similar to in-memory data structures is pretty powerful. Consider using it [instead of SQLite](https://gist.github.com/radarroark/03a0724484e1111ef4c05d72a935c42c) for your Java projects: it's simpler, it's pure Java, and it creates no impedance mismatch with your program the way SQL databases do.

* [Example](#example)
* [Initializing a Database](#initializing-a-database)
* [Types](#types)
* [Cloning and Undoing](#cloning-and-undoing)
* [Large Byte Arrays](#large-byte-arrays)
* [Iterators](#iterators)
* [Hashing](#hashing)
* [Compaction](#compaction)
* [Thread Safety](#thread-safety)

## Example

In this example, we create a new database, write some data in a transaction, and read the data afterwards.

```java
try (var raf = new RandomAccessBufferedFile(new File("main.db"), "rw")) {
    // init the db
    var core = new CoreBufferedFile(raf);
    var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
    var db = new Database(core, hasher);

    // to get the benefits of immutability, the top-level data structure
    // must be an ArrayList, so each transaction is stored as an item in it
    var history = new WriteArrayList(db.rootCursor());

    // this is how a transaction is executed. we call history.appendContext,
    // providing it with the most recent copy of the db and a context
    // object. the context object has a method that will run before the
    // transaction has completed. this method is where we can write
    // changes to the db. if any error happens in it, the transaction
    // will not complete and the db will be unaffected.
    //
    // after this transaction, the db will look like this if represented
    // as JSON (in reality the format is binary):
    //
    // {"foo": "foo",
    //  "bar": "bar",
    //  "fruits": ["apple", "pear", "grape"],
    //  "people": [
    //    {"name": "Alice", "age": 25},
    //    {"name": "Bob", "age": 42}
    //  ]}
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

    // to get the "fruits" list, we get the cursor to it and
    // then pass it to the ArrayList constructor
    var fruitsCursor = moment.getCursor("fruits");
    var fruits = new ReadArrayList(fruitsCursor);
    assertEquals(3, fruits.count());

    // now we can get the first item from the fruits list and read it
    var appleCursor = fruits.getCursor(0);
    var appleValue = appleCursor.readBytes(MAX_READ_BYTES);
    assertEquals("apple", new String(appleValue));
}
```

## Initializing a Database

A `Database` is initialized with an implementation of the `Core` interface, which determines how the i/o is done. There are three implementations of `Core` in this library: `CoreBufferedFile`, `CoreFile`, and `CoreMemory`.

* `CoreBufferedFile` databases, like in the example above, write to a file while using an in-memory buffer to dramatically improve performance. This is highly recommended if you want to create a file-based database.
* `CoreFile` databases use no buffering when reading and writing data. You can initialize it like in the example above, except with a `RandomAccessFile` instance. This is almost never necessary but it's useful as a benchmark comparison with `CoreBufferedFile` databases.
* `CoreMemory` databases work completely in memory. You can initialize it like in the example above, except with a `RandomAccessMemory` instance.

Usually, you want to use a top-level `ArrayList` like in the example above, because that allows you to store a reference to each copy of the database (which I call a "moment"). This is how it supports transactions, despite not having any rollback journal or write-ahead log. It's an append-only database, so the data you are writing is invisible to any reader until the very last step, when the top-level list's header is updated.

You can also use a top-level `HashMap`, which is useful for ephemeral databases where immutability or transaction safety isn't necessary. Since xitdb supports in-memory databases, you could use it as an over-the-wire serialization format. Much like "Cap'n Proto", xitdb has no encoding/decoding step: you just give the buffer to xitdb and it can immediately read from it.

## Types

In xitdb there are a variety of immutable data structures that you can nest arbitrarily:

* `HashMap` contains key-value pairs stored with a hash
* `HashSet` is like a `HashMap` that only sets the keys; it is useful when only checking for membership
* `CountedHashMap` and `CountedHashSet` are just a `HashMap` and `HashSet` that maintain a count of their contents
* `ArrayList` is a growable array
* `LinkedArrayList` is like an `ArrayList` that can also be efficiently sliced and concatenated

All data structures use the hash array mapped trie, invented by Phil Bagwell. The `LinkedArrayList` is based on his later work on RRB trees. These data structures were originally made immutable and widely available by Rich Hickey in Clojure. To my knowledge, they haven't been available in any open source database until xitdb.

There are also scalar types you can store in the above-mentioned data structures:

* `Bytes` is a byte array
* `Uint` is an unsigned 64-bit int
* `Int` is a signed 64-bit int
* `Float` is a 64-bit float

You may also want to define custom types. For example, you may want to store a big integer that can't fit in 64 bits. You could just store this with `Bytes`, but when reading the byte array there wouldn't be any indication that it should be interpreted as a big integer.

In xitdb, you can optionally store a format tag with a byte array. A format tag is a 2 byte tag that is stored alongside the byte array. Readers can use it to decide how to interpret the byte array. Here's an example of storing a random 256-bit number with `bi` as the format tag:

```java
var randomBigInt = new BigInteger(256, new java.util.Random());
moment.put("random-number", new Database.Bytes(randomBigInt.toByteArray(), "bi".getBytes()));
```

Then, you can read it like this:

```java
var randomNumberCursor = moment.getCursor("random-number");
var randomNumber = randomNumberCursor.readBytesObject(MAX_READ_BYTES);
assertEquals("bi", new String(randomNumber.formatTag()));
var randomBigInt = new BigInteger(randomNumber.value());
```

There are many types you may want to store this way. Maybe an ISO-8601 date like `2026-01-01T18:55:48Z` could be stored with `dt` as the format tag. It's also great for storing custom classes. Just define the class, serialize it as a byte array using whatever mechanism you wish, and store it with a format tag. Keep in mind that format tags can be *any* 2 bytes, so there are 65536 possible format tags.

## Cloning and Undoing

A powerful feature of immutable data is fast cloning. Any data structure can be instantly cloned and changed without affecting the original. Starting with the example code above, we can make a new transaction that creates a "food" list based on the existing "fruits" list:

```java
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
```

Before we continue, let's save the latest history index, so we can revert back to this moment of the database later:

```java
var historyIndex = history.count() - 1;
```

There's one catch you'll run into when cloning. If we try cloning a data structure that was created in the same transaction, it doesn't seem to work:

```java
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
```

The reason that `big-cities` was mutated is because all data in a given transaction is temporarily mutable. This is a very important optimization, but in this case, it's not what we want.

To show how to fix this, let's first undo the transaction we just made. Here we use the `historyIndex` we saved before to revert back to the older database moment:

```java
history.append(history.getSlot(historyIndex));
```

This time, after making the "big cities" list, we call `freeze`, which tells xitdb to consider all data made so far in the transaction to be immutable. After that, we can clone it into the "cities" list and it will work the way we wanted:

```java
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
```

## Large Byte Arrays

When reading and writing large byte arrays, you probably don't want to have all of their contents in memory at once. To incrementally write to a byte array, just get a writer from a cursor:

```java
var longTextCursor = moment.putCursor("long-text");
var cursorWriter = longTextCursor.writer();
try (var bos = new BufferedOutputStream(cursorWriter)) {
    for (int i = 0; i < 50; i++) {
        bos.write("hello, world\n".getBytes());
    }
}
cursorWriter.finish(); // remember to call this!
```

If you need to set a format tag for the byte array, put it in the `formatTag` field of the writer before you call `finish`.

To read a byte array incrementally, get a reader from a cursor:

```java
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
```

## Iterators

All data structures support iteration. Here's an example of iterating over an `ArrayList` and printing all of the keys and values of each `HashMap` contained in it:

```java
var peopleCursor = moment.getCursor("people");
var people = new ReadArrayList(peopleCursor);

var peopleIter = people.iterator();
while (peopleIter.hasNext()) {
    var personCursor = peopleIter.next();
    var person = new ReadHashMap(personCursor);
    var personIter = person.iterator();
    while (personIter.hasNext()) {
        var kvPairCursor = personIter.next();
        var kvPair = kvPairCursor.readKeyValuePair();

        var key = new String(kvPair.keyCursor.readBytes(MAX_READ_BYTES));

        switch (kvPair.valueCursor.slot().tag()) {
            case SHORT_BYTES, BYTES -> System.out.println(key + ": " + new String(kvPair.valueCursor.readBytes(MAX_READ_BYTES)));
            case UINT -> System.out.println(key + ": " + kvPair.valueCursor.readUint());
            case INT -> System.out.println(key + ": " + kvPair.valueCursor.readInt());
            case FLOAT -> System.out.println(key + ": " + kvPair.valueCursor.readFloat());
            default -> throw new Database.UnexpectedTagException();
        }
    }
}
```

The above code iterates over `people`, which is an `ArrayList`, and for each person (which is a `HashMap`), it iterates over each of its key-value pairs.

The iteration of the `HashMap` looks the same with `HashSet`, `CountedHashMap`, and `CountedHashSet`. When iterating, you call `readKeyValuePair` on the cursor and can read the `keyCursor` and `valueCursor` from it. In maps, `put` sets the key and value. In sets, `put` only sets the key; the value will always have a tag type of `NONE`.

## Hashing

The hashing data structures will create the hash for you when you call methods like `put` or `getCursor` and provide the key as a `String` or a `Database.Bytes`. If you want to do the hashing yourself, there is an overload of those methods that take a `byte[]` as the key, which should be the hash that you computed.

When initializing a database, you tell xitdb how to hash with the `Hasher`. If you're using SHA-1, it will look like this:

```java
try (var raf = new RandomAccessFile(new File("main.db"), "rw")) {
    var core = new CoreFile(raf);
    var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
    var db = new Database(core, hasher);
    // ...
}
```

The size of the hash in bytes will be stored in the database's header. If you try opening it later with a hashing algorithm that has the wrong hash size, it will throw an exception. If you are unsure what hash size the database uses, this creates a chicken-and-egg problem. You can read the header before initializing the database like this:

```java
core.seek(0);
var header = Database.Header.read(core);
assertEquals(20, header.hashSize());
```

The hash size alone does not disambiguate hashing algorithms, though. In addition, xitdb reserves four bytes in the header that you can use to put the name of the algorithm. You must provide it in the `Hasher` constructor:

```java
var hasher = new Hasher(MessageDigest.getInstance("SHA-1"), Hasher.stringToId("sha1"));
```

The hash id is only written to the database header when it is first initialized. When you open it later, the hash id in the `Hasher` is ignored. You can read the hash id of an existing database like this:

```java
core.seek(0);
var header = Database.Header.read(core);
assertEquals("sha1", Hasher.idToString(header.hashId()));
```

If you want to use SHA-256, I recommend using `sha2` as the hash id. You can then distinguish between SHA-256 and SHA-512 using the hash size, like this:

```java
Hasher hasher = switch (Hasher.idToString(header.hashId())) {
    case "sha1" -> new Hasher(MessageDigest.getInstance("SHA-1"), header.hashId());
    case "sha2" -> switch (header.hashSize()) {
        case 32 -> new Hasher(MessageDigest.getInstance("SHA-256"), header.hashId());
        case 64 -> new Hasher(MessageDigest.getInstance("SHA-512"), header.hashId());
        default -> throw new RuntimeException("Invalid hash size");
    };
    default -> throw new RuntimeException("Invalid hash algorithm");
};
assertEquals("SHA-1", hasher.md().getAlgorithm());
```

## Compaction

Normally, an immutable database grows forever, because old data is never deleted. To reclaim disk space and clear the history, xitdb supports compaction. This involves completely rebuilding the database file to only contain the data accessible from the latest copy (i.e., "moment") of the database.

```java
// create the file and core for the new database
var compactFile = new RandomAccessBufferedFile(new File("compact.db"), "rw");
var compactCore = new CoreBufferedFile(compactFile);

var compactDb = db.compact(compactCore);

// read from the new compacted db
var history = new ReadArrayList(compactDb.rootCursor());
assertEquals(1, history.count());
```

## Thread Safety

It is possible to read the database from multiple threads without locks, even while writes are happening. This is a big benefit of immutable databases. However, each thread needs to use its own `Database` instance. You can do this by creating a `ThreadLocal`. See [the multithreading test](https://github.com/xit-vcs/xitdb-java/blob/d7cf0869cf0f66eca823051dfbdec0ab5e5a09cb/src/test/java/io/github/radarroark/xitdb/DatabaseTest.java#L201) for an example of this. Also, keep in mind that writes still need to come from one thread at a time.
