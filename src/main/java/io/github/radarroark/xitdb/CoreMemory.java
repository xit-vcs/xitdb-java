package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CoreMemory implements Core {
    public RandomAccessMemory memory;

    public CoreMemory(RandomAccessMemory memory) {
        this.memory = memory;
    }

    @Override
    public DataInput reader() {
        return this.memory;
    }

    @Override
    public DataOutput writer() {
        return this.memory;
    }

    @Override
    public long length() throws IOException {
        return this.memory.size();
    }

    @Override
    public void seek(long pos) throws IOException {
        this.memory.seek((int)pos);
    }

    @Override
    public long position() throws IOException {
        return this.memory.position.get();
    }

    @Override
    public void setLength(long len) throws IOException {
        this.memory.setLength((int)len);
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void sync() throws IOException {
    }

    @Override
    public void close() throws IOException {
        this.memory.close();
    }
}
