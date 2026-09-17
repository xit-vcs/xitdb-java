package io.github.radarroark.xitdb;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class RandomAccessMemory extends ByteArrayOutputStream implements DataOutput, DataInput {
    // per-thread positions; synchronized access to shared bytes and size
    ThreadLocal<Integer> position;

    public RandomAccessMemory() {
        this.position = new ThreadLocal<>() {
            @Override
            protected Integer initialValue() {
                return 0;
            }
        };
    }

    public synchronized void seek(int pos) {
        if (pos > this.count) {
            this.position.set(this.count);
        } else {
            this.position.set(pos);
        }
    }

    public synchronized void setLength(int len) throws IOException {
        if (len < 0 || len > this.count) throw new IllegalArgumentException();
        // update the position first, since boxing can allocate.
        if (len == 0 || this.position.get() > len) {
            this.position.set(len);
        }
        this.count = len;
    }

    // the backing array, which is valid up to size().
    // unlike toByteArray, this doesn't make a copy.
    synchronized byte[] buffer() {
        return this.buf;
    }

    // positional access, which doesn't use the per-thread position.
    // for callers that track their own position, this avoids a
    // thread-local lookup and a boxed integer on every call.

    synchronized void writeAt(int pos, byte[] buffer) {
        if (pos < 0 || pos > this.count) throw new IndexOutOfBoundsException();
        int bytesBeforeEnd = Math.min(buffer.length, this.count - pos);
        System.arraycopy(buffer, 0, this.buf, pos, bytesBeforeEnd);
        super.write(buffer, bytesBeforeEnd, buffer.length - bytesBeforeEnd);
    }

    synchronized void readAt(int pos, byte[] b, int off, int len) throws IOException {
        if (pos > this.count - len) throw new EOFException();
        System.arraycopy(this.buf, pos, b, off, len);
    }

    // ByteArrayOutputStream

    @Override
    public synchronized void reset() {
        super.reset();
        this.position.set(0);
    }

    // DataOutput

    @Override
    public synchronized void write(byte[] buffer) throws IOException {
        int pos = this.position.get();
        // another thread may have truncated the memory since this one's seek
        writeAt(Math.min(pos, this.count), buffer);
        this.position.set(pos + buffer.length);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeBoolean'");
    }

    @Override
    public void writeByte(int v) throws IOException {
        write(new byte[]{(byte) (v & 0b1111_1111)});
    }

    @Override
    public void writeShort(int v) throws IOException {
        var buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) (v & 0b1111_1111_1111_1111));
        write(buffer.array());
    }

    @Override
    public void writeChar(int v) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeChar'");
    }

    @Override
    public void writeInt(int v) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeInt'");
    }

    @Override
    public void writeLong(long v) throws IOException {
        var buffer = ByteBuffer.allocate(8);
        buffer.putLong(v);
        write(buffer.array());
    }

    @Override
    public void writeFloat(float v) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeFloat'");
    }

    @Override
    public void writeDouble(double v) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeDouble'");
    }

    @Override
    public void writeBytes(String s) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeBytes'");
    }

    @Override
    public void writeChars(String s) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeChars'");
    }

    @Override
    public void writeUTF(String s) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeUTF'");
    }

    // DataInput

    @Override
    public void readFully(byte[] b) throws IOException {
        this.readFully(b, 0, b.length);
    }

    @Override
    public synchronized void readFully(byte[] b, int off, int len) throws IOException {
        int pos = this.position.get();
        readAt(pos, b, off, len);
        this.position.set(pos + len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'skipBytes'");
    }

    @Override
    public boolean readBoolean() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readBoolean'");
    }

    @Override
    public byte readByte() throws IOException {
        var bytes = new byte[1];
        this.readFully(bytes);
        return bytes[0];
    }

    @Override
    public int readUnsignedByte() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readUnsignedByte'");
    }

    @Override
    public short readShort() throws IOException {
        var bytes = new byte[2];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readUnsignedShort'");
    }

    @Override
    public char readChar() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readChar'");
    }

    @Override
    public int readInt() throws IOException {
        var bytes = new byte[4];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException {
        var bytes = new byte[8];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getLong();
    }

    @Override
    public float readFloat() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readFloat'");
    }

    @Override
    public double readDouble() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readDouble'");
    }

    @Override
    public String readLine() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readLine'");
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readUTF'");
    }
}
