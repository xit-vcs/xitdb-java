package io.github.radarroark.xitdb;

public enum Tag {
    NONE,
    INDEX,
    ARRAY_LIST,
    LINKED_ARRAY_LIST,
    HASH_MAP,
    KV_PAIR,
    BYTES,
    SHORT_BYTES,
    UINT,
    INT,
    FLOAT,
    HASH_SET,
    COUNTED_HASH_MAP,
    COUNTED_HASH_SET,
    SORTED_MAP,
    SORTED_SET;

    public static Tag valueOf(int n) {
        // validate the tag so corrupted data yields an error
        // instead of an ArrayIndexOutOfBoundsException
        var values = Tag.values();
        if (n < 0 || n >= values.length) {
            throw new Database.UnexpectedTagException();
        }
        return values[n];
    }
}
