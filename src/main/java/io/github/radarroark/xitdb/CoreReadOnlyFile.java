package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.WeakHashMap;

/**
 * read-only file access with a separate handle and position per thread.
 * returned readers are thread-confined. close after active reads have finished.
 * this isolates file access, not the state of a shared database.
 */
public class CoreReadOnlyFile implements Core {
    private final File file;
    private final ThreadLocal<RandomAccessFile> local = new ThreadLocal<>();
    private final WeakHashMap<RandomAccessFile, Boolean> handles = new WeakHashMap<>();
    private volatile boolean closed;

    public CoreReadOnlyFile(File file) throws IOException {
        this.file = file.getAbsoluteFile();
        current();
    }

    private void checkOpen() {
        if (closed) throw new IllegalStateException("core is closed");
    }

    private RandomAccessFile current() throws IOException {
        checkOpen();
        var handle = local.get();
        if (handle != null) return handle;

        synchronized (handles) {
            checkOpen();
            handle = new RandomAccessFile(file, "r");
            // weak keys allow terminated threads' handles to be collected
            handles.put(handle, Boolean.TRUE);
            local.set(handle);
            return handle;
        }
    }

    @Override
    public DataInput reader() {
        try {
            return current();
        } catch (IOException e) {
            // core.reader cannot declare a checked exception
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public DataOutput writer() {
        checkOpen();
        throw new UnsupportedOperationException("read-only core");
    }

    @Override
    public long length() throws IOException {
        return current().length();
    }

    @Override
    public void seek(long pos) throws IOException {
        current().seek(pos);
    }

    @Override
    public long position() throws IOException {
        return current().getFilePointer();
    }

    @Override
    public void setLength(long len) throws IOException {
        checkOpen();
        throw new UnsupportedOperationException("read-only core");
    }

    @Override
    public void flush() throws IOException {
        checkOpen();
    }

    @Override
    public void sync() throws IOException {
        checkOpen();
    }

    @Override
    public void close() throws IOException {
        synchronized (handles) {
            if (closed) return;
            closed = true;
            IOException failure = null;
            for (var handle : handles.keySet()) {
                try {
                    handle.close();
                } catch (IOException e) {
                    if (failure == null) failure = e;
                    else failure.addSuppressed(e);
                }
            }
            handles.clear();
            local.remove();
            if (failure != null) throw failure;
        }
    }
}
