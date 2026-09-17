package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class RandomAccessBufferedFile implements DataOutput, DataInput, AutoCloseable {
    public RandomAccessFile file;
    RandomAccessMemory memory;
    int bufferSize; // flushes before the memory would grow beyond this size
    long filePos;
    long memoryPos;
    // the file's length, cached so that `length` doesn't need to ask the OS
    // every time data is allocated. another process may write to the file
    // whenever this one isn't, so it is only set once we begin writing, and
    // it is cleared when the writes are flushed.
    Long fileLen = null;

    public RandomAccessBufferedFile(File file, String mode) throws FileNotFoundException {
        this(file, mode, 8 * 1024 * 1024);
    }

    public RandomAccessBufferedFile(File file, String mode, int bufferSize) throws FileNotFoundException {
        this.file = new RandomAccessFile(file, mode);
        this.memory = new RandomAccessMemory();
        this.bufferSize = bufferSize;
        this.filePos = 0;
        this.memoryPos = 0;
    }

    /**
     * Returns the underlying file channel for locking.
     * Direct channel I/O bypasses this file's buffering and logical position.
     */
    public FileChannel getChannel() {
        return this.file.getChannel();
    }

    public void seek(long pos) throws IOException {
        this.filePos = pos;
    }

    public long length() throws IOException {
        long fileLen = this.fileLen != null ? this.fileLen : this.file.length();
        var bufferSize = this.memory.size();
        // a failed allocation or a rollback can leave an empty
        // buffer positioned beyond the file's end.
        if (bufferSize == 0) return fileLen;
        return Math.max(this.memoryPos + bufferSize, fileLen);
    }

    public long position() throws IOException {
        return this.filePos;
    }

    public void setLength(long len) throws IOException {
        // discard buffered bytes past the new end rather than flushing them.
        // a rollback must not depend on writing the data it is throwing away,
        // because that write may be what failed (e.g. the disk is full).
        if (len <= this.memoryPos) {
            this.memory.reset();
        } else if (len < this.memoryPos + this.memory.size()) {
            this.memory.setLength((int) (len - this.memoryPos));
        }
        this.fileLen = null;
        this.file.setLength(len);
        this.filePos = Math.min(len, this.filePos);
    }

    public void flush() throws IOException {
        this.fileLen = null;
        if (this.memory.size() > 0) {
            writeToFile(this.memoryPos, this.memory.buffer(), 0, this.memory.size());
            this.memory.reset();
        }
    }

    // the channel reads and writes at a given position, which avoids the separate
    // seek that RandomAccessFile needs. it copies through a native buffer that it
    // keeps for the life of the thread, so large transfers are done in chunks.
    private static final int MAX_TRANSFER_SIZE = 1024 * 1024;

    private void writeToFile(long pos, byte[] buffer, int off, int len) throws IOException {
        // if the write fails partway, the file's length is unknown
        var fileLen = this.fileLen;
        this.fileLen = null;

        var channel = this.file.getChannel();
        var src = ByteBuffer.wrap(buffer, off, len);
        int end = off + len;
        while (src.position() < end) {
            src.limit(src.position() + Math.min(end - src.position(), MAX_TRANSFER_SIZE));
            channel.write(src, pos + (src.position() - off));
        }

        if (fileLen != null) this.fileLen = Math.max(fileLen, pos + len);
    }

    private void readFromFile(long pos, byte[] buffer, int off, int len) throws IOException {
        var channel = this.file.getChannel();
        var dst = ByteBuffer.wrap(buffer, off, len);
        int end = off + len;
        while (dst.position() < end) {
            dst.limit(dst.position() + Math.min(end - dst.position(), MAX_TRANSFER_SIZE));
            if (channel.read(dst, pos + (dst.position() - off)) < 0) throw new EOFException();
        }
    }

    public void sync() throws IOException {
        flush();
        this.file.getFD().sync();
    }

    // AutoCloseable

    @Override
    public void close() throws IOException {
        try {
            flush();
        } finally {
            // close the file even if the flush fails, so that
            // its handle and any lock on it are released
            this.file.close();
            this.memory.close();
        }
    }

    // DataOutput

    @Override
    public void write(byte[] buffer) throws IOException {
        if (buffer.length == 0) return;

        // the in-memory buffer is a single contiguous window of the file
        // starting at memoryPos. start a new window at this position if
        // the buffer is empty, the write is past the end of the window,
        // or the write would grow the window beyond the max size.
        var memorySize = this.memory.size();
        if (memorySize == 0
                || this.filePos > this.memoryPos + memorySize
                || (this.filePos >= this.memoryPos && this.filePos - this.memoryPos + buffer.length > this.bufferSize)) {
            this.flush();
            this.memoryPos = this.filePos;
        }

        if (this.fileLen == null) {
            this.fileLen = this.file.length();
        }

        if (this.filePos >= this.memoryPos && this.filePos - this.memoryPos + buffer.length <= this.bufferSize) {
            // write to the in-memory buffer
            this.memory.seek((int) (this.filePos - this.memoryPos));
            this.memory.write(buffer);
        } else {
            // a direct disk write that overlaps the buffered region would be
            // clobbered by a later flush of stale buffer bytes, so flush first
            if (this.filePos < this.memoryPos + this.memory.size() && this.filePos + buffer.length > this.memoryPos) {
                this.flush();
            }
            writeToFile(this.filePos, buffer, 0, buffer.length);
        }

        this.filePos += buffer.length;
    }

    @Override
    public void write(int i) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'write'");
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeBoolean'");
    }

    @Override
    public void writeByte(int i) throws IOException {
        var b = ByteBuffer.allocate(1);
        b.put((byte) i);
        this.write(b.array());
    }

    @Override
    public void writeShort(int i) throws IOException {
        var buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) (i & 0b1111_1111_1111_1111));
        this.write(buffer.array());
    }

    @Override
    public void writeChar(int i) throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'writeChar'");
    }

    @Override
    public void writeInt(int i) throws IOException {
        var buffer = ByteBuffer.allocate(4);
        buffer.putInt(i);
        this.write(buffer.array());
    }

    @Override
    public void writeLong(long l) throws IOException {
        var buffer = ByteBuffer.allocate(8);
        buffer.putLong(l);
        this.write(buffer.array());
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
    public void readFully(byte[] buffer) throws IOException {
        int pos = 0;

        // read from the disk -- before the in-memory buffer
        if (this.filePos < this.memoryPos) {
            // compare as longs, because the buffer can be more than 2 GiB away
            int sizeBeforeMem = (int) Math.min(this.memoryPos - this.filePos, (long) buffer.length);
            readFromFile(this.filePos, buffer, 0, sizeBeforeMem);
            pos += sizeBeforeMem;
            this.filePos += sizeBeforeMem;
        }

        if (pos == buffer.length) return;

        // read from the in-memory buffer
        if (this.filePos >= this.memoryPos && this.filePos < this.memoryPos + this.memory.size()) {
            int memPos = (int) (this.filePos - this.memoryPos);
            int sizeInMem = Math.min(this.memory.size() - memPos, buffer.length - pos);
            this.memory.seek(memPos);
            this.memory.readFully(buffer, pos, sizeInMem);
            pos += sizeInMem;
            this.filePos += sizeInMem;
        }

        if (pos == buffer.length) return;

        // read from the disk -- after the in-memory buffer
        if (this.filePos >= this.memoryPos + this.memory.size()) {
            int sizeAfterMem = (int) (buffer.length - pos);
            readFromFile(this.filePos, buffer, pos, sizeAfterMem);
            pos += sizeAfterMem;
            this.filePos += sizeAfterMem;
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        var buffer = new byte[len];
        this.readFully(buffer);
        System.arraycopy(buffer, 0, b, off, len);
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
        var b = new byte[1];
        this.readFully(b);
        return b[0];
    }

    @Override
    public int readUnsignedByte() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readUnsignedByte'");
    }

    @Override
    public short readShort() throws IOException {
        var b = new byte[2];
        this.readFully(b);
        return ByteBuffer.wrap(b).getShort();
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
        var b = new byte[4];
        this.readFully(b);
        return ByteBuffer.wrap(b).getInt();
    }

    @Override
    public long readLong() throws IOException {
        var b = new byte[8];
        this.readFully(b);
        return ByteBuffer.wrap(b).getLong();
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
