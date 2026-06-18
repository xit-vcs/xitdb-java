package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public class RandomAccessBufferedFile implements DataOutput, DataInput, AutoCloseable {
    RandomAccessFile file;
    RandomAccessMemory memory;
    int bufferSize; // flushes when the memory is >= this size
    long filePos;
    long memoryPos;

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

    public void seek(long pos) throws IOException {
        // flush if we are going past the end of the in-memory buffer
        if (pos > this.memoryPos + this.memory.size()) {
            this.flush();
        }

        this.filePos = pos;

        // if the buffer is empty, set its position to this offset as well
        if (this.memory.size() == 0) {
            this.memoryPos = pos;
        }
    }

    public long length() throws IOException {
        return Math.max(this.memoryPos + this.memory.size(), this.file.length());
    }

    public long position() throws IOException {
        return this.filePos;
    }

    public void setLength(long len) throws IOException {
        flush();
        this.file.setLength(len);
        this.filePos = Math.min(len, this.filePos);
    }

    public void flush() throws IOException {
        if (this.memory.size() > 0) {
            this.file.seek(this.memoryPos);
            this.file.write(this.memory.toByteArray());

            this.memoryPos = 0;
            this.memory.reset();
        }
    }

    public void sync() throws IOException {
        flush();
        this.file.getFD().sync();
    }

    // AutoCloseable

    @Override
    public void close() throws Exception {
        flush();
        this.file.close();
        this.memory.close();
    }

    // DataOutput

    @Override
    public void write(byte[] buffer) throws IOException {
        if (this.memory.size() + buffer.length > this.bufferSize) {
            this.flush();
        }

        if (this.filePos >= this.memoryPos && this.filePos <= this.memoryPos + this.memory.size()) {
            this.memory.seek((int) (this.filePos - this.memoryPos));
            this.memory.write(buffer);
        } else {
            // a direct disk write that overlaps the buffered region would be
            // clobbered by a later flush of stale buffer bytes, so flush first
            if (this.filePos < this.memoryPos + this.memory.size() && this.filePos + buffer.length > this.memoryPos) {
                this.flush();
            }
            this.file.seek(this.filePos);
            this.file.write(buffer);
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
            int sizeBeforeMem = Math.min((int) (this.memoryPos - this.filePos), buffer.length);
            this.file.seek(this.filePos);
            this.file.readFully(buffer, 0, sizeBeforeMem);
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
            this.file.seek(this.filePos);
            this.file.readFully(buffer, pos, sizeAfterMem);
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
