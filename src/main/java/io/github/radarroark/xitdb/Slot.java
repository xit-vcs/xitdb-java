package io.github.radarroark.xitdb;

import java.nio.ByteBuffer;

public record Slot(long value, Tag tag, boolean full) implements Database.WriteableData {
    public static int length = 9;

    public Slot() {
        this(0, Tag.NONE, false);
    }

    public Slot(long value, Tag tag) {
        this(value, tag, false);
    }

    public Slot withTag(Tag tag) {
        return new Slot(this.value, tag, this.full);
    }

    public Slot withFull(boolean full) {
        return new Slot(this.value, this.tag, full);
    }

    public boolean empty() {
        return this.tag == Tag.NONE && !this.full;
    }

    /**
     * returns the offset of a separately stored value, or null for inline values.
     * this constant-time operation performs no i/o
     * and does not freeze the referenced data.
     */
    public Long valueOffset() {
        return switch (this.tag) {
            case NONE, UINT, INT, FLOAT, SHORT_BYTES -> null;
            case INDEX, ARRAY_LIST, LINKED_ARRAY_LIST, HASH_MAP, KV_PAIR, BYTES,
                 HASH_SET, COUNTED_HASH_MAP, COUNTED_HASH_SET, SORTED_MAP, SORTED_SET -> this.value;
        };
    }

    public byte[] toBytes() {
        var buffer = ByteBuffer.allocate(length);
        var tagInt = this.full ? 0b1000_0000 : 0;
        tagInt = tagInt | this.tag.ordinal();
        buffer.put((byte)tagInt);
        buffer.putLong(this.value);
        return buffer.array();
    }

    public static Slot fromBytes(byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);
        var tagByte = buffer.get();
        var full = (tagByte & 0b1000_0000) != 0;
        var tag = Tag.valueOf(tagByte & 0b0111_1111);
        var value = buffer.getLong();
        return new Slot(value, tag, full);
    }
}
