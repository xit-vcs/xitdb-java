package io.github.radarroark.xitdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import org.junit.jupiter.api.Test;

class RandomAccessMemoryTest {
    @Test
    void shrinkingPreservesPrefixAndClampsPosition() throws Exception {
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            core.writer().write(new byte[]{1, 2, 3, 4});
            core.setLength(2);
            assertEquals(2, core.length());
            assertEquals(2, core.position());
            assertThrows(EOFException.class, () -> core.reader().readByte());
            core.seek(0);
            assertEquals(1, core.reader().readByte());
            assertEquals(2, core.reader().readByte());
            core.writer().writeByte(9);
            assertArrayEquals(new byte[]{1, 2, 9}, core.memory.toByteArray());
        }
    }

    @Test
    void shrinkingAndKeepingLengthPreserveEarlierPosition() throws Exception {
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            core.writer().write(new byte[]{1, 2, 3, 4});
            core.seek(1);
            core.setLength(3);
            assertEquals(1, core.position());
            core.setLength(3);
            assertEquals(1, core.position());
            assertEquals(2, core.reader().readByte());
            assertArrayEquals(new byte[]{1, 2, 3}, core.memory.toByteArray());
        }
    }

    @Test
    void clearingResetsPositionAndAllowsReuse() throws Exception {
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            core.writer().write(new byte[]{1, 2, 3});
            core.setLength(0);
            assertEquals(0, core.length());
            assertEquals(0, core.position());
            assertThrows(EOFException.class, () -> core.reader().readByte());
            core.setLength(0);
            core.writer().writeByte(9);
            assertArrayEquals(new byte[]{9}, core.memory.toByteArray());
        }
    }

    @Test
    void invalidLengthsLeaveContentsAndPositionUntouched() throws Exception {
        try (var core = new CoreMemory(new RandomAccessMemory())) {
            core.writer().write(new byte[]{1, 2, 3});
            core.seek(1);
            for (int length : new int[]{4, -1, Integer.MIN_VALUE}) {
                assertThrows(IllegalArgumentException.class, () -> core.memory.setLength(length));
                assertEquals(1, core.position());
                assertArrayEquals(new byte[]{1, 2, 3}, core.memory.toByteArray());
            }
        }
    }
}
