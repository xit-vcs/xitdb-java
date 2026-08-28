package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Core extends AutoCloseable {
    public DataInput reader();

    public DataOutput writer();

    public long length() throws IOException;

    public void seek(long pos) throws IOException;

    public long position() throws IOException;

    public void setLength(long len) throws IOException;

    public void flush() throws IOException;

    public void sync() throws IOException;

    @Override
    public void close() throws IOException;
}
