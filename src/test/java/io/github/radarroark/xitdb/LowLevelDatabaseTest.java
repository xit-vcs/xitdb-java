package io.github.radarroark.xitdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

class LowLevelDatabaseTest {
    static long MAX_READ_BYTES = 1024;

    @Test
    void testLowLevelApi() throws Exception {
        try (var ram = new RandomAccessMemory()) {
            var core = new CoreMemory(ram);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            testLowLevelApi(core, hasher);
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();

            try (var raf = new RandomAccessFile(file, "rw")) {
                var core = new CoreFile(raf);
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testLowLevelApi(core, hasher);
            }
        }

        {
            var file = File.createTempFile("database", "");
            file.deleteOnExit();

            try (var raf = new RandomAccessBufferedFile(file, "rw", 1024)) {
                var core = new CoreBufferedFile(raf);
                var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
                testLowLevelApi(core, hasher);
            }
        }
    }

    @Test
    void testLowLevelMemoryOperations() throws Exception {
        try (var ram = new RandomAccessMemory()) {
            var core = new CoreMemory(ram);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            var db = new Database(core, hasher);

            var hash = db.md.digest("text".getBytes("UTF-8"));
            var textCursor = db.rootCursor().writePath(new Database.PathPart[]{
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(hash)),
            });

            var writer = textCursor.writer();
            writer.write("goodbye, world!".getBytes());
            writer.seek(9);
            writer.write("cruel world!".getBytes());
            writer.finish();

            var reader = textCursor.reader();
            assertEquals("goodbye, cruel world!", new String(reader.readAllBytes()));

            core.seek(core.length() + 1);
            assertThrows(EOFException.class, () -> core.reader().readFully(new byte[1]));
        }
    }

    void testSlice(Core core, Hasher hasher, int originalSize, long sliceOffset, long sliceSize) throws Exception {
        core.setLength(0);
        var db = new Database(core, hasher);
        var rootCursor = db.rootCursor();

        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                var values = new ArrayList<Long>();

                // create list
                for (int i = 0; i < originalSize; i++) {
                    long n = i * 2;
                    values.add(n);
                    cursor.writePath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes()))),
                        new Database.LinkedArrayListInit(),
                        new Database.LinkedArrayListAppend(),
                        new Database.WriteData(new Database.Uint(n))
                    });
                }

                // slice list
                var evenListCursor = cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes())))
                });
                var evenListSliceCursor = cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-slice".getBytes()))),
                    new Database.WriteData(evenListCursor.slotPtr.slot()),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListSlice(sliceOffset, sliceSize)
                });

                // check all the values in the new slice
                for (int i = 0; i < sliceSize; i++) {
                    var val = values.get((int) sliceOffset + i);
                    var n = cursor.readPath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-slice".getBytes()))),
                        new Database.LinkedArrayListGet(i)
                    }).slotPtr.slot().value();
                    assertEquals(val, n);
                }

                // check all values in the new slice with an iterator
                {
                    var iter = evenListSliceCursor.iterator();
                    int i = 0;
                    while (iter.hasNext()) {
                        var numCursor = iter.next();
                        assertEquals(values.get((int)sliceOffset + i), numCursor.readUint());
                        i += 1;
                    }
                    assertEquals(sliceSize, i);
                }

                // there are no extra items
                assertEquals(null, cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-slice".getBytes()))),
                    new Database.LinkedArrayListGet(sliceSize)
                }));

                // concat the slice with itself
                cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                    new Database.WriteData(evenListSliceCursor.slotPtr.slot()),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListConcat(evenListSliceCursor.slotPtr.slot())
                });

                // check all values in the combo list
                var comboValues = new ArrayList<Long>();
                comboValues.addAll(values.subList((int) sliceOffset, (int) (sliceOffset + sliceSize)));
                comboValues.addAll(values.subList((int) sliceOffset, (int) (sliceOffset + sliceSize)));
                for (int i = 0; i < comboValues.size(); i++) {
                    var n = cursor.readPath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                        new Database.LinkedArrayListGet(i)
                    }).slotPtr.slot().value();
                    assertEquals(comboValues.get(i), n);
                }

                // append to the slice
                cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-slice".getBytes()))),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListAppend(),
                    new Database.WriteData(new Database.Uint(3))
                });

                // read the new value from the slice
                assertEquals(3, cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-slice".getBytes()))),
                    new Database.LinkedArrayListGet(-1)
                }).slotPtr.slot().value());
            })
        });
    }

    void testConcat(Core core, Hasher hasher, long listASize, long listBSize) throws Exception {
        core.setLength(0);
        var db = new Database(core, hasher);
        var rootCursor = db.rootCursor();

        var values = new ArrayList<Long>();

        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                // create even list
                cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes()))),
                    new Database.LinkedArrayListInit()
                });
                for (int i = 0; i < listASize; i++) {
                    long n = i * 2;
                    values.add(n);
                    cursor.writePath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes()))),
                        new Database.LinkedArrayListInit(),
                        new Database.LinkedArrayListAppend(),
                        new Database.WriteData(new Database.Uint(n))
                    });
                }

                // create odd list
                cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("odd".getBytes()))),
                    new Database.LinkedArrayListInit()
                });
                for (int i = 0; i < listBSize; i++) {
                    long n = (i * 2) + 1;
                    values.add(n);
                    cursor.writePath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("odd".getBytes()))),
                        new Database.LinkedArrayListInit(),
                        new Database.LinkedArrayListAppend(),
                        new Database.WriteData(new Database.Uint(n))
                    });
                }
            })
        });

        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                // get the even list
                var evenListCursor = cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes())))
                });

                // get the odd list
                var oddListCursor = cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("odd".getBytes())))
                });

                // concat the lists
                var comboListCursor = cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                    new Database.WriteData(evenListCursor.slotPtr.slot()),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListConcat(oddListCursor.slotPtr.slot())
                });

                // check all values in the new list
                for (int i = 0; i < values.size(); i++) {
                    var n = cursor.readPath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                        new Database.LinkedArrayListGet(i)
                    }).slotPtr.slot().value();
                    assertEquals(values.get(i), n);
                }

                // check all values in the new slice with an iterator
                {
                    var iter = comboListCursor.iterator();
                    int i = 0;
                    while (iter.hasNext()) {
                        var numCursor = iter.next();
                        assertEquals(values.get(i), numCursor.readUint());
                        i += 1;
                    }
                    assertEquals(evenListCursor.count() + oddListCursor.count(), i);
                }

                // there are no extra items
                assertEquals(null, cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                    new Database.LinkedArrayListGet(values.size())
                }));
            })
        });
    }

    void testInsertAndRemove(Core core, Hasher hasher, int originalSize, long insertIndex) throws Exception {
        core.setLength(0);
        var db = new Database(core, hasher);
        var rootCursor = db.rootCursor();

        long insertValue = 12345;

        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                var values = new ArrayList<Long>();

                // create list
                for (int i = 0; i < originalSize; i++) {
                    if (i == insertIndex) {
                        values.add(insertValue);
                    }
                    long n = i * 2;
                    values.add(n);
                    cursor.writePath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes()))),
                        new Database.LinkedArrayListInit(),
                        new Database.LinkedArrayListAppend(),
                        new Database.WriteData(new Database.Uint(n))
                    });
                }

                // insert into list
                var evenListCursor = cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes())))
                });
                var evenListInsertCursor = cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-insert".getBytes()))),
                    new Database.WriteData(evenListCursor.slotPtr.slot()),
                    new Database.LinkedArrayListInit(),
                });
                evenListInsertCursor.writePath(new Database.PathPart[]{
                    new Database.LinkedArrayListInsert(insertIndex),
                    new Database.WriteData(new Database.Uint(insertValue))
                });

                // check all the values in the new list
                for (int i = 0; i < values.size(); i++) {
                    var val = values.get(i);
                    var n = cursor.readPath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-insert".getBytes()))),
                        new Database.LinkedArrayListGet(i)
                    }).slotPtr.slot().value();
                    assertEquals(val, n);
                }

                // check all values in the new list with an iterator
                {
                    var iter = evenListInsertCursor.iterator();
                    int i = 0;
                    while (iter.hasNext()) {
                        var numCursor = iter.next();
                        assertEquals(values.get(i), numCursor.readUint());
                        i += 1;
                    }
                    assertEquals(values.size(), i);
                }

                // there are no extra items
                assertEquals(null, cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-insert".getBytes()))),
                    new Database.LinkedArrayListGet(values.size())
                }));
            })
        });

        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                var values = new ArrayList<Long>();

                for (int i = 0; i < originalSize; i++) {
                    long n = i * 2;
                    values.add(n);
                }

                // remove inserted value from the list
                var evenListInsertCursor = cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-insert".getBytes()))),
                    new Database.LinkedArrayListRemove(insertIndex),
                });

                // check all the values in the new list
                for (int i = 0; i < values.size(); i++) {
                    var val = values.get(i);
                    var n = cursor.readPath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-insert".getBytes()))),
                        new Database.LinkedArrayListGet(i)
                    }).slotPtr.slot().value();
                    assertEquals(val, n);
                }

                // check all values in the new list with an iterator
                {
                    var iter = evenListInsertCursor.iterator();
                    int i = 0;
                    while (iter.hasNext()) {
                        var numCursor = iter.next();
                        assertEquals(values.get(i), numCursor.readUint());
                        i += 1;
                    }
                    assertEquals(values.size(), i);
                }

                // there are no extra items
                assertEquals(null, cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-insert".getBytes()))),
                    new Database.LinkedArrayListGet(values.size())
                }));
            })
        });
    }

    void testLowLevelApi(Core core, Hasher hasher) throws Exception {
        // open and re-open database
        {
            // make empty database
            core.setLength(0);
            new Database(core, hasher);

            // re-open without error
            var db = new Database(core, hasher);
            var writer = db.core.writer();
            db.core.seek(0);
            writer.writeByte('g');

            // re-open with error
            assertThrows(Database.InvalidDatabaseException.class, () -> new Database(core, hasher));

            // modify the version
            db.core.seek(0);
            writer.writeByte('x');
            db.core.seek(4);
            writer.writeShort(Database.VERSION + 1);

            // re-open with error
            assertThrows(Database.InvalidVersionException.class, () -> new Database(core, hasher));
        }

        // save hash id in header
        {
            var hashId = Hasher.stringToId("sha1");
            var hasherWithHashId = new Hasher(MessageDigest.getInstance("SHA-1"), hashId);

            // make empty database
            core.setLength(0);
            new Database(core, hasherWithHashId);

            // read header without initializing database
            core.seek(0);
            var header = Database.Header.read(core);
            assertEquals(20, header.hashSize());
            assertEquals("sha1", Hasher.idToString(header.hashId()));

            // determine the hashing algorithm
            Hasher hash = switch (Hasher.idToString(header.hashId())) {
                case "sha1" -> new Hasher(MessageDigest.getInstance("SHA-1"), header.hashId());
                case "sha2" -> switch (header.hashSize()) {
                    case 32 -> new Hasher(MessageDigest.getInstance("SHA-256"), header.hashId());
                    case 64 -> new Hasher(MessageDigest.getInstance("SHA-512"), header.hashId());
                    default -> throw new RuntimeException("Invalid hash size");
                };
                default -> throw new RuntimeException("Invalid hash algorithm");
            };
            assertEquals("SHA-1", hash.md().getAlgorithm());
        }

        // array_list of hash_maps
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            // write foo -> bar with a writer
            var fooKey = db.md.digest("foo".getBytes());
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.Context((cursor) -> {
                    assertEquals(Tag.NONE, cursor.slot().tag());
                    var writer = cursor.writer();
                    writer.write("bar".getBytes());
                    writer.finish();
                })
            });

            // read foo
            {
                var barCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                });
                assertEquals(3, barCursor.count());
                var barValue = barCursor.readBytes(MAX_READ_BYTES);
                assertEquals("bar", new String(barValue));

                // TODO: test wrapping ReadCursor.Reader in a BufferedReader
            }

            // read foo from ctx
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.Context((cursor) -> {
                    assertNotEquals(Tag.NONE, cursor.slot().tag());

                    var value = cursor.readBytes(MAX_READ_BYTES);
                    assertEquals("bar", new String(value));

                    var barReader = cursor.reader();

                    // read into buffer
                    var barBytes = new byte[10];
                    var barSize = barReader.read(barBytes);
                    assertEquals("bar", new String(barBytes, 0, barSize));
                    barReader.seek(0);
                    assertEquals(3, barReader.read(barBytes));
                    assertEquals("bar", new String(barBytes, 0, 3));

                    // read one char at a time
                    {
                        var ch = new byte[1];
                        barReader.seek(0);

                        barReader.readFully(ch);
                        assertEquals("b", new String(ch));

                        barReader.readFully(ch);
                        assertEquals("a", new String(ch));

                        barReader.readFully(ch);
                        assertEquals("r", new String(ch));

                        assertThrows(Database.EndOfStreamException.class, () -> barReader.readFully(ch));

                        barReader.seek(1);
                        assertEquals('a', (char)barReader.readByte());

                        barReader.seek(0);
                        assertEquals('b', (char)barReader.readByte());
                    }
                })
            });

            // overwrite foo -> baz
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.Context((cursor) -> {
                    assertNotEquals(Tag.NONE, cursor.slot().tag());

                    var writer = cursor.writer();
                    writer.write("x".getBytes());
                    writer.write("x".getBytes());
                    writer.write("x".getBytes());
                    writer.seek(0);
                    writer.write("b".getBytes());
                    writer.seek(2);
                    writer.write("z".getBytes());
                    writer.seek(1);
                    writer.write("a".getBytes());
                    writer.finish();

                    var value = cursor.readBytes(MAX_READ_BYTES);
                    assertEquals("baz", new String(value));
                })
            });

            // if error in ctx, db doesn't change
            {
                var sizeBefore = core.length();

                try {
                    rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                        new Database.Context((cursor) -> {
                            var writer = cursor.writer();
                            writer.write("this value won't be visible".getBytes());
                            writer.finish();
                            throw new Exception();
                        })
                    });
                } catch (Exception e) {}

                // read foo
                var valueCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                });
                var value = valueCursor.readBytes(null); // make sure null max size works
                assertEquals("baz", new String(value));

                // verify that the db is properly truncated back to its original size after error
                var sizeAfter = core.length();
                assertEquals(sizeBefore, sizeAfter);
            }

            // write bar -> longstring
            var barKey = db.md.digest("bar".getBytes());
            {
                var barCursor = rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                });
                barCursor.write(new Database.Bytes("longstring"));

                // the slot tag is BYTES because the byte array is > 8 bytes long
                assertEquals(Tag.BYTES, barCursor.slot().tag());

                // writing again returns the same slot
                {
                    var nextBarCursor = rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    nextBarCursor.writeIfEmpty(new Database.Bytes("longstring"));
                    assertEquals(barCursor.slot().value(), nextBarCursor.slot().value());
                }

                // writing with write returns a new slot
                {
                    var nextBarCursor = rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    nextBarCursor.write(new Database.Bytes("longstring"));
                    assertNotEquals(barCursor.slot().value(), nextBarCursor.slot().value());
                }
            }

            // read bar
            {
                var readBarCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                });
                var barValue = readBarCursor.readBytes(MAX_READ_BYTES);
                assertEquals("longstring", new String(barValue));
            }

            // write bar -> shortstr
            {
                var barCursor = rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                });
                barCursor.write(new Database.Bytes("shortstr"));

                // the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long
                assertEquals(Tag.SHORT_BYTES, barCursor.slot().tag());
                assertEquals(8, barCursor.count());

                // make sure that SHORT_BYTES can be read with a reader
                var barReader = barCursor.reader();
                var barValue = new byte[(int)barCursor.count()];
                barReader.readFully(barValue);
                assertEquals("shortstr", new String(barValue));
            }

            // write bytes with a format tag
            {
                // shortstr
                {
                    var barCursor = rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    barCursor.write(new Database.Bytes("shortstr", "st"));

                    // the slot tag is BYTES because the byte array is > 8 bytes long including the format tag
                    assertEquals(Tag.BYTES, barCursor.slot().tag());
                    assertEquals(8, barCursor.count());

                    // read bar
                    var readBarCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    var barBytes = readBarCursor.readBytesObject(MAX_READ_BYTES);
                    assertEquals("shortstr", new String(barBytes.value()));
                    assertEquals("st", new String(barBytes.formatTag()));

                    // make sure that BYTES can be read with a reader
                    var barReader = barCursor.reader();
                    var barValue = new byte[(int)barCursor.count()];
                    barReader.readFully(barValue);
                    assertEquals("shortstr", new String(barValue));
                }

                // shorts
                {
                    var barCursor = rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    barCursor.write(new Database.Bytes("shorts", "st"));

                    // the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long including the format tag
                    assertEquals(Tag.SHORT_BYTES, barCursor.slot().tag());
                    assertEquals(6, barCursor.count());

                    // read bar
                    var readBarCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    var barBytes = readBarCursor.readBytesObject(MAX_READ_BYTES);
                    assertEquals("shorts", new String(barBytes.value()));
                    assertEquals("st", new String(barBytes.formatTag()));

                    // make sure that SHORT_BYTES can be read with a reader
                    var barReader = barCursor.reader();
                    var barValue = new byte[(int)barCursor.count()];
                    barReader.readFully(barValue);
                    assertEquals("shorts", new String(barValue));
                }

                // short
                {
                    var barCursor = rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    barCursor.write(new Database.Bytes("short", "st"));

                    // the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long including the format tag
                    assertEquals(Tag.SHORT_BYTES, barCursor.slot().tag());
                    assertEquals(5, barCursor.count());

                    // read bar
                    var readBarCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    var barBytes = readBarCursor.readBytesObject(MAX_READ_BYTES);
                    assertEquals("short", new String(barBytes.value()));
                    assertEquals("st", new String(barBytes.formatTag()));

                    // make sure that SHORT_BYTES can be read with a reader
                    var barReader = barCursor.reader();
                    var barValue = new byte[(int)barCursor.count()];
                    barReader.readFully(barValue);
                    assertEquals("short", new String(barValue));
                }
            }

            // read foo into buffer
            {
                var barCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                });
                var barBufferValue = barCursor.readBytes(MAX_READ_BYTES);
                assertEquals("baz", new String(barBufferValue));
            }

            // write bar and get a pointer to it
            var barSlot = rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(barKey)),
                new Database.WriteData(new Database.Bytes("bar"))
            }).slot();

            // overwrite foo -> bar using the bar pointer
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.WriteData(barSlot)
            });
            var barCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
            });
            var barValue = barCursor.readBytes(MAX_READ_BYTES);
            assertEquals("bar", new String(barValue));

            // can still read the old value
            var bazCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-2),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
            });
            var bazValue = bazCursor.readBytes(MAX_READ_BYTES);
            assertEquals("baz", new String(bazValue));

            // key not found
            var notFoundKey = db.md.digest("this doesn't exist".getBytes());
            assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-2),
                new Database.HashMapGet(new Database.HashMapGetValue(notFoundKey))
            }));

            // write key that conflicts with foo the first two bytes
            var smallConflictKey = db.md.digest("small conflict".getBytes());
            smallConflictKey[smallConflictKey.length-1] = fooKey[fooKey.length-1];
            smallConflictKey[smallConflictKey.length-2] = fooKey[fooKey.length-2];
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(smallConflictKey)),
                new Database.WriteData(new Database.Bytes("small"))
            });

            // write key that conflicts with foo the first four bytes
            var conflictKey = db.md.digest("conflict".getBytes());
            conflictKey[conflictKey.length-1] = fooKey[fooKey.length-1];
            conflictKey[conflictKey.length-2] = fooKey[fooKey.length-2];
            conflictKey[conflictKey.length-3] = fooKey[fooKey.length-3];
            conflictKey[conflictKey.length-4] = fooKey[fooKey.length-4];
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey)),
                new Database.WriteData(new Database.Bytes("hello"))
            });

            // read conflicting key
            var helloCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey))
            });
            var helloValue = helloCursor.readBytes(MAX_READ_BYTES);
            assertEquals("hello", new String(helloValue));

            // we can still read foo
            var barCursor2 = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
            });
            var barValue2 = barCursor2.readBytes(MAX_READ_BYTES);
            assertEquals("bar", new String(barValue2));

            // overwrite conflicting key
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey)),
                new Database.WriteData(new Database.Bytes("goodbye"))
            });
            var goodbyeCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey))
            });
            var goodbyeValue = goodbyeCursor.readBytes(MAX_READ_BYTES);
            assertEquals("goodbye", new String(goodbyeValue));

            // we can still read the old conflicting key
            var helloCursor2 = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-2),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey))
            });
            var helloValue2 = helloCursor2.readBytes(MAX_READ_BYTES);
            assertEquals("hello", new String(helloValue2));

            // remove the conflicting keys
            {
                // foo's slot is an INDEX slot due to the conflict
                {
                    var mapCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                    });
                    var indexPos = mapCursor.slot().value();
                    assertEquals(Tag.HASH_MAP, mapCursor.slot().tag());

                    var reader = core.reader();

                    var i = new BigInteger(fooKey).and(Database.BIG_MASK).intValueExact();
                    var slotPos = indexPos + (Slot.length * i);
                    core.seek(slotPos);
                    var slotBytes = new byte[Slot.length];
                    reader.readFully(slotBytes);
                    var slot = Slot.fromBytes(slotBytes);

                    assertEquals(Tag.INDEX, slot.tag());
                }

                // remove the small conflict key
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapRemove(smallConflictKey)
                });

                // the conflict key still exists in history
                assertNotEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-2),
                    new Database.HashMapGet(new Database.HashMapGetValue(smallConflictKey)),
                }));

                // the conflict key doesn't exist in the latest moment
                assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(smallConflictKey)),
                }));

                // the other conflict key still exists
                assertNotEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(conflictKey)),
                }));

                // foo's slot is still an INDEX slot due to the other conflicting key
                {
                    var mapCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                    });
                    var indexPos = mapCursor.slot().value();
                    assertEquals(Tag.HASH_MAP, mapCursor.slot().tag());

                    var reader = core.reader();

                    var i = new BigInteger(fooKey).and(Database.BIG_MASK).intValueExact();
                    var slotPos = indexPos + (Slot.length * i);
                    core.seek(slotPos);
                    var slotBytes = new byte[Slot.length];
                    reader.readFully(slotBytes);
                    var slot = Slot.fromBytes(slotBytes);

                    assertEquals(Tag.INDEX, slot.tag());
                }

                // remove the conflict key
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapRemove(conflictKey)
                });

                // the conflict keys don't exist in the latest moment
                assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(smallConflictKey)),
                }));
                assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(conflictKey)),
                }));

                // foo's slot is now a KV_PAIR slot, becaus the branch was shortened
                {
                    var mapCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                    });
                    var indexPos = mapCursor.slot().value();
                    assertEquals(Tag.HASH_MAP, mapCursor.slot().tag());

                    var reader = core.reader();

                    var i = new BigInteger(fooKey).and(Database.BIG_MASK).intValueExact();
                    var slotPos = indexPos + (Slot.length * i);
                    core.seek(slotPos);
                    var slotBytes = new byte[Slot.length];
                    reader.readFully(slotBytes);
                    var slot = Slot.fromBytes(slotBytes);

                    assertEquals(Tag.KV_PAIR, slot.tag());
                }
            }

            {
                // overwrite foo with a uint
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                    new Database.WriteData(new Database.Uint(42))
                });

                // read foo
                var uintValue = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                }).readUint();
                assertEquals(42, uintValue);
            }

            {
                // overwrite foo with a int
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                    new Database.WriteData(new Database.Int(-42))
                });

                // read foo
                var intValue = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                }).readInt();
                assertEquals(-42, intValue);
            }

            {
                // overwrite foo with a float
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                    new Database.WriteData(new Database.Float(42.5))
                });

                // read foo
                var intValue = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                }).readFloat();
                assertEquals(42.5, intValue);
            }

            // remove foo
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapRemove(fooKey)
            });

            // remove key that does not exist
            assertThrows(Database.KeyNotFoundException.class, () -> rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapRemove(db.md.digest("doesn't exist".getBytes()))
            }));

            // make sure foo doesn't exist anymore
            assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
            }));

            // non-top-level list
            {
                // write apple
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes("apple"))
                });

                // read apple
                var appleCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(-1),
                });
                var appleValue = appleCursor.readBytes(MAX_READ_BYTES);
                assertEquals("apple", new String(appleValue));

                // write banana
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes("banana"))
                });

                // read banana
                var bananaCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(-1),
                });
                var bananaValue = bananaCursor.readBytes(MAX_READ_BYTES);
                assertEquals("banana", new String(bananaValue));

                // can't read banana in older array_list
                assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-2),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(1),
                }));

                // write pear
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes("pear"))
                });

                // write grape
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes("grape"))
                });

                // read pear
                var pearCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(-2),
                });
                var pearValue = pearCursor.readBytes(MAX_READ_BYTES);
                assertEquals("pear", new String(pearValue));

                // read grape
                var grapeCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(-1),
                });
                var grapeValue = grapeCursor.readBytes(MAX_READ_BYTES);
                assertEquals("grape", new String(grapeValue));
            }
        }

        // append to top-level array_list many times, filling up the array_list until a root overflow occurs
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            var watKey = db.md.digest("wat".getBytes());
            for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                var value = "wat" + i;
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                    new Database.WriteData(new Database.Bytes(value))
                });
            }

            for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                var value = "wat" + i;
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(i),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // add more slots to cause a new index block to be created.
            // a new index block will be created when i == 32 (the 33rd append).
            // during that transaction, return an error so the transaction is
            // cancelled, causing truncation to happen. this test ensures that
            // the new index block is NOT truncated. this is prevented by updating
            // the file size in the header immediately after making a new index block.
            // see `readArrayListSlot` for more.
            for (int i = Database.SLOT_COUNT + 1; i < Database.SLOT_COUNT * 2 + 1; i++) {
                var value = "wat" + i;

                final int index = i;

                try {
                    rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                        new Database.WriteData(new Database.Bytes(value)),
                        new Database.Context((cursor) -> {
                            if (index == 32) {
                                throw new Exception();
                            }
                        })
                    });
                } catch (Exception e) {}
            }

            // try another append to make sure we still can.
            // if truncation destroyed the index block, this would fail.
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                new Database.WriteData(new Database.Bytes("wat32"))
            });

            // slice so it contains exactly SLOT_COUNT,
            // so we have the old root again
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListSlice(Database.SLOT_COUNT)
            });

            // we can iterate over the remaining slots
            for (int i = 0; i < Database.SLOT_COUNT; i ++) {
                var value = "wat" + i;
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(i),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey))
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // but we can't get the value that we sliced out of the array list
            assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(Database.SLOT_COUNT + 1)
            }));
        }

        // append to inner array_list many times, filling up the array_list until a root overflow occurs
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                var value = "wat" + i;
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes(value))
                });
            }

            for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                var value = "wat" + i;
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(i),
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // slice the inner array list so it contains exactly SLOT_COUNT,
            // so we have the old root again
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListGet(-1),
                new Database.ArrayListInit(),
                new Database.ArrayListSlice(Database.SLOT_COUNT)
            });

            // we can iterate over the remaining slots
            for (int i = 0; i < Database.SLOT_COUNT; i ++) {
                var value = "wat" + i;
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(i),
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // but we can't get the value that we sliced out of the array list
            assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.ArrayListGet(Database.SLOT_COUNT + 1)
            }));

            // overwrite the last value with hello
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.ArrayListInit(),
                new Database.ArrayListGet(-1),
                new Database.WriteData(new Database.Bytes("hello"))
            });

            // read last value
            {
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(-1)
                });
                var value = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals("hello", value);
            }

            // overwrite the last value with goodbye
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.ArrayListInit(),
                new Database.ArrayListGet(-1),
                new Database.WriteData(new Database.Bytes("goodbye"))
            });

            // read last value
            {
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(-1)
                });
                var value = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals("goodbye", value);
            }

            // previous last value is still hello
            {
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-2),
                    new Database.ArrayListGet(-1)
                });
                var value = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals("hello", value);
            }
        }

        // iterate over inner array_list
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            // add wats
            for (int i = 0; i < 10; i++) {
                var value = "wat" + i;
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes(value))
                });

                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(-1)
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // iterate over array_list
            {
                var innerCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1)
                });
                var iter = innerCursor.iterator();
                int i = 0;
                while (iter.hasNext()) {
                    var nextCursor = iter.next();
                    var value = "wat" + i;
                    var value2 = new String(nextCursor.readBytes(MAX_READ_BYTES));
                    assertEquals(value, value2);
                    i += 1;
                }
                assertEquals(10, i);
            }

            // set first slot to .none and make sure iteration still works.
            // this validates that it correctly returns .none slots if
            // their flag is set, rather than skipping over them.
            {
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListInit(),
                    new Database.ArrayListGet(0),
                    new Database.WriteData(null)
                });
                var innerCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1)
                });
                var iter = innerCursor.iterator();
                int i = 0;
                while (iter.hasNext()) {
                    iter.next();
                    i += 1;
                }
                assertEquals(10, i);
            }

            // get list slot
            var listCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1)
            });
            assertEquals(10, listCursor.count());
        }

        // iterate over inner hash_map
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            // add wats
            for (int i = 0; i < 10; i++) {
                var value = "wat" + i;
                var watKey = db.md.digest(value.getBytes());
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                    new Database.WriteData(new Database.Bytes(value))
                });

                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey))
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // add foo
            var fooKey = db.md.digest("foo".getBytes());
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetKey(fooKey)),
                new Database.WriteData(new Database.Bytes("foo"))
            });
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.WriteData(new Database.Uint(42))
            });

            // remove a wat
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapRemove(db.md.digest("wat0".getBytes()))
            });

            // iterate over hash_map
            {
                var innerCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1)
                });
                var iter = innerCursor.iterator();
                int i = 0;
                while (iter.hasNext()) {
                    var kvPairCursor = iter.next();
                    var kvPair = kvPairCursor.readKeyValuePair();
                    if (Arrays.equals(kvPair.hash, fooKey)) {
                        var key = new String(kvPair.keyCursor.readBytes(MAX_READ_BYTES));
                        assertEquals("foo", key);
                        assertEquals(42, kvPair.valueCursor.slotPtr.slot().value());
                    } else {
                        var value = kvPair.valueCursor.readBytes(MAX_READ_BYTES);
                        assert(Arrays.equals(kvPair.hash, db.md.digest(value)));
                    }
                    i += 1;
                }
                assertEquals(10, i);
            }

            // iterate over hash_map with writeable cursor
            {
                var innerCursor = rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                });
                var iter = innerCursor.iterator();
                int i = 0;
                while (iter.hasNext()) {
                    var kvPairCursor = iter.next();
                    var kvPair = kvPairCursor.readKeyValuePair();
                    if (Arrays.equals(kvPair.hash, fooKey)) {
                        kvPair.keyCursor.write(new Database.Bytes("bar"));
                    }
                    i += 1;
                }
                assertEquals(10, i);
            }
        }

        {
            // slice linked_array_list
            testSlice(core, hasher, Database.SLOT_COUNT * 5 + 1, 10, 5);
            testSlice(core, hasher, Database.SLOT_COUNT * 5 + 1, 0, Database.SLOT_COUNT * 2);
            testSlice(core, hasher, Database.SLOT_COUNT * 5, Database.SLOT_COUNT * 3, Database.SLOT_COUNT);
            testSlice(core, hasher, Database.SLOT_COUNT * 5, Database.SLOT_COUNT * 3, Database.SLOT_COUNT * 2);
            testSlice(core, hasher, Database.SLOT_COUNT * 2, 10, Database.SLOT_COUNT);
            testSlice(core, hasher, 2, 0, 2);
            testSlice(core, hasher, 2, 1, 1);
            testSlice(core, hasher, 1, 0, 0);

            // concat linked_array_list
            testConcat(core, hasher, Database.SLOT_COUNT * 5 + 1, Database.SLOT_COUNT + 1);
            testConcat(core, hasher, Database.SLOT_COUNT, Database.SLOT_COUNT);
            testConcat(core, hasher, 1, 1);
            testConcat(core, hasher, 0, 0);

            // insert linked_array_list
            testInsertAndRemove(core, hasher, 1, 0);
            testInsertAndRemove(core, hasher, 10, 0);
            testInsertAndRemove(core, hasher, 10, 5);
            testInsertAndRemove(core, hasher, 10, 9);
            testInsertAndRemove(core, hasher, Database.SLOT_COUNT * 5, Database.SLOT_COUNT * 2);
        }

        // concat linked_array_list multiple times
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.Context((cursor) -> {
                    var values = new ArrayList<Long>();

                    // create list
                    for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                        long n = i * 2;
                        values.add(n);
                        cursor.writePath(new Database.PathPart[]{
                            new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes()))),
                            new Database.LinkedArrayListInit(),
                            new Database.LinkedArrayListAppend(),
                            new Database.WriteData(new Database.Uint(n))
                        });
                    }

                    // get list slot
                    var evenListCursor = cursor.readPath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes())))
                    });
                    assertEquals(Database.SLOT_COUNT + 1, evenListCursor.count());

                    // check all values in the new slice with an iterator
                    {
                        var innerCursor = cursor.readPath(new Database.PathPart[]{
                            new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes())))
                        });
                        var iter = innerCursor.iterator();
                        int i = 0;
                        while (iter.hasNext()) {
                            iter.next();
                            i += 1;
                        }
                        assertEquals(Database.SLOT_COUNT + 1, i);
                    }

                    // concat the list with itself multiple times.
                    // since each list has 17 items, each concat
                    // will create a gap, causing a root overflow
                    // before a normal array list would've.
                    var comboListCursor = cursor.writePath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                        new Database.WriteData(evenListCursor.slotPtr.slot()),
                        new Database.LinkedArrayListInit()
                    });
                    for (int i = 0; i < 16; i++) {
                        comboListCursor = comboListCursor.writePath(new Database.PathPart[]{
                            new Database.LinkedArrayListConcat(evenListCursor.slotPtr.slot())
                        });
                    }

                    // append to the new list
                    cursor.writePath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                        new Database.LinkedArrayListAppend(),
                        new Database.WriteData(new Database.Uint(3))
                    });

                    // read the new value from the list
                    assertEquals(3, cursor.readPath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                        new Database.LinkedArrayListGet(-1)
                    }).slotPtr.slot().value());

                    // append more to the new list
                    for (int i = 0; i < 500; i++) {
                        cursor.writePath(new Database.PathPart[]{
                            new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("combo".getBytes()))),
                            new Database.LinkedArrayListAppend(),
                            new Database.WriteData(new Database.Uint(1))
                        });
                    }
                })
            });
        }

        // append items to linked_array_list without setting their value
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            // appending without setting any value should work
            for (int i = 0; i < 8; i++) {
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListAppend()
                });
            }

            // explicitly writing a null slot should also work
            for (int i = 0; i < 8; i++) {
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListAppend(),
                    new Database.WriteData(null)
                });
            }
        }

        // insert at beginning of linked_array_list many times
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.LinkedArrayListInit(),
                new Database.LinkedArrayListAppend(),
                new Database.WriteData(new Database.Uint(42))
            });

            for (int i = 0; i < 1_000; i++) {
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListInsert(0),
                    new Database.WriteData(new Database.Uint(i))
                });
            }
        }

        // insert at end of linked_array_list many times
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.LinkedArrayListInit(),
                new Database.LinkedArrayListAppend(),
                new Database.WriteData(new Database.Uint(42))
            });

            for (int i = 0; i < 1_000; i++) {
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListInsert(i),
                    new Database.WriteData(new Database.Uint(i))
                });
            }
        }
    }
}
