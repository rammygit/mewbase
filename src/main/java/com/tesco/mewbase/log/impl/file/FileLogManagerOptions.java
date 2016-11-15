package com.tesco.mewbase.log.impl.file;

/**
 * Created by tim on 08/10/16.
 */
public class FileLogManagerOptions {

    public static final String DEFAULT_LOG_DIR = "mewlog";
    public static final int DEFAULT_MAX_LOG_CHUNK_SIZE = 4 * 10 * 1024 * 1024;
    public static final int DEFAULT_PREALLOCATE_SIZE = 0;
    public static final int DEFAULT_MAX_RECORD_SIZE = 4 * 1024 * 1024;
    public static final int DEFAULT_READ_BUFFER_SIZE = 4 * 1024;

    private String logDir = DEFAULT_LOG_DIR;
    private int maxLogChunkSize = DEFAULT_MAX_LOG_CHUNK_SIZE;
    private int preallocateSize = DEFAULT_PREALLOCATE_SIZE;
    private int maxRecordSize = DEFAULT_MAX_RECORD_SIZE;
    private int readBufferSize = DEFAULT_READ_BUFFER_SIZE;

    public String getLogDir() {
        return logDir;
    }

    public FileLogManagerOptions setLogDir(String logDir) {
        this.logDir = logDir;
        return this;
    }

    public int getMaxLogChunkSize() {
        return maxLogChunkSize;
    }

    public FileLogManagerOptions setMaxLogChunkSize(int maxLogChunkSize) {
        this.maxLogChunkSize = maxLogChunkSize;
        return this;
    }

    public int getMaxRecordSize() {
        return maxRecordSize;
    }

    public FileLogManagerOptions setMaxRecordSize(int maxRecordSize) {
        this.maxRecordSize = maxRecordSize;
        return this;
    }

    public int getPreallocateSize() {
        return preallocateSize;
    }

    public FileLogManagerOptions setPreallocateSize(int preallocateSize) {
        this.preallocateSize = preallocateSize;
        return this;
    }

    public int getReadBufferSize() {
        return readBufferSize;
    }

    public FileLogManagerOptions setReadBufferSize(int readBufferSize) {
        this.readBufferSize = readBufferSize;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FileLogManagerOptions that = (FileLogManagerOptions) o;

        if (maxLogChunkSize != that.maxLogChunkSize) return false;
        if (preallocateSize != that.preallocateSize) return false;
        if (maxRecordSize != that.maxRecordSize) return false;
        if (readBufferSize != that.readBufferSize) return false;
        return logDir.equals(that.logDir);

    }

    @Override
    public int hashCode() {
        int result = logDir.hashCode();
        result = 31 * result + maxLogChunkSize;
        result = 31 * result + preallocateSize;
        result = 31 * result + maxRecordSize;
        result = 31 * result + readBufferSize;
        return result;
    }
}
