package io.github.radarroark.xitdb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MemoryRollbackTest {
    @Test
    void rollbackPreservesCommittedHistoryWithLimitedHeap(@TempDir Path directory) throws Exception {
        var output = directory.resolve("rollback.log");
        var classpath = new File(Database.class.getProtectionDomain().getCodeSource().getLocation().toURI())
            + File.pathSeparator
            + new File(MemoryRollbackTest.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        // Keep heap pressure isolated from the test runner. G1 makes the
        // available space independent of the host's default collector.
        var process = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx128m", "-XX:+UseG1GC", "-cp", classpath, Probe.class.getName())
            .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Rollback probe timed out");
            assertEquals(0, process.exitValue(), Files.readString(output));
        } finally {
            process.destroyForcibly();
        }
    }

    // Runs without JUnit in a separate JVM, through the public transaction API.
    public static class Probe {
        public static void main(String[] args) throws Exception {
            try (var core = new CoreMemory(new RandomAccessMemory())) {
                var db = new Database(core, new Hasher(MessageDigest.getInstance("SHA-1")));
                var history = new WriteArrayList(db.rootCursor());
                history.appendContext(new Slot(), cursor -> {
                    new WriteHashMap(cursor).put("generation", new Database.Int(0));
                });
                var payload = new byte[1024 * 1024];
                Arrays.fill(payload, (byte) 'x');
                for (int i = 1; i <= 40; i++) {
                    final int generation = i;
                    history.appendContext(history.getSlot(-1), cursor -> {
                        var map = new WriteHashMap(cursor);
                        map.put("generation", new Database.Int(generation));
                        map.put("payload", new Database.Bytes(payload));
                    });
                }
                System.gc();
                long before = core.length();
                var abort = new RuntimeException("deliberate abort");
                Throwable failure = null;
                try {
                    history.appendContext(history.getSlot(-1), cursor -> {
                        new WriteHashMap(cursor).put("temporary", new Database.Int(1));
                        throw abort;
                    });
                } catch (Throwable e) {
                    failure = e;
                }
                System.out.println("before=" + before + ", after=" + core.length() + ", failure=" + failure);
                if (failure != abort || before != core.length()) {
                    throw new AssertionError("Rollback must preserve committed storage and the original exception", failure);
                }
                // Reopen and check every retained moment, including the payload bytes.
                var reopened = new Database(core, new Hasher(MessageDigest.getInstance("SHA-1")));
                var committed = new ReadArrayList(reopened.rootCursor());
                if (committed.count() != 41) throw new AssertionError("History count changed");
                for (int i = 0; i <= 40; i++) {
                    var map = new ReadHashMap(committed.getCursor(i));
                    if (map.getCursor("generation").readInt() != i) throw new AssertionError("History changed");
                    if (map.getSlot("temporary") != null) throw new AssertionError("Aborted value was committed");
                    if (i > 0 && !Arrays.equals(payload, map.getCursor("payload").readBytes((long) payload.length))) {
                        throw new AssertionError("Committed payload changed");
                    }
                }
                history.appendContext(history.getSlot(-1), cursor -> {
                    new WriteHashMap(cursor).put("generation", new Database.Int(41));
                });
                if (history.count() != 42
                    || new ReadHashMap(history.getCursor(-1)).getCursor("generation").readInt() != 41) {
                    throw new AssertionError("Cannot commit after rollback");
                }
            }
        }
    }
}
