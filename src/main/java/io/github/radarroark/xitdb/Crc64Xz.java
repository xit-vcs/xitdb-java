package io.github.radarroark.xitdb;

// CRC-64/XZ: the reflected form of the ECMA-182 polynomial, with all bits of
// the initial value and final xor set. java has no built-in CRC-64.
final class Crc64Xz {
    private static final long POLYNOMIAL = 0xC96C5795D7870F42L;
    private static final long[] TABLE = new long[256];

    static {
        for (int i = 0; i < TABLE.length; i++) {
            long crc = i;
            for (int bit = 0; bit < 8; bit++) {
                crc = (crc & 1) != 0 ? (crc >>> 1) ^ POLYNOMIAL : crc >>> 1;
            }
            TABLE[i] = crc;
        }
    }

    private Crc64Xz() {}

    static long hash(byte[] bytes) {
        long crc = 0xFFFFFFFFFFFFFFFFL;
        for (byte b : bytes) {
            crc = TABLE[(int) ((crc ^ b) & 0xFF)] ^ (crc >>> 8);
        }
        return crc ^ 0xFFFFFFFFFFFFFFFFL;
    }
}
