package com.tesco.mewbase.server;


import io.vertx.core.net.NetServerOptions;

import java.util.Arrays;

/**
 * Created by tim on 22/09/16.
 */
public class ServerOptions {

    public static final String DEFAULT_HOST = "0.0.0.0";
    public static final int DEFAULT_PORT = 7451;
    public static final String DEFAULT_DOCS_DIR = "docs";
    private String[] channels;
    private NetServerOptions netServerOptions = new NetServerOptions().setPort(DEFAULT_PORT).setHost(DEFAULT_HOST);
    private String docsDir = DEFAULT_DOCS_DIR;
    private String[] binders;
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

    public String[] getChannels() {
        return channels;
    }

    public ServerOptions setChannels(String[] channels) {
        this.channels = channels;
        return this;
    }

    public NetServerOptions getNetServerOptions() {
        return netServerOptions;
    }

    public ServerOptions setNetServerOptions(NetServerOptions netServerOptions) {
        this.netServerOptions = netServerOptions;
        return this;
    }

    public String getDocsDir() {
        return docsDir;
    }

    public ServerOptions setDocsDir(String docsDir) {
        this.docsDir = docsDir;
        return this;
    }

    public String[] getBinders() {
        return binders;
    }

    public ServerOptions setBinders(String[] binders) {
        this.binders = binders;
        return this;
    }

    public String getLogDir() {
        return logDir;
    }

    public ServerOptions setLogDir(String logDir) {
        this.logDir = logDir;
        return this;
    }

    public int getMaxLogChunkSize() {
        return maxLogChunkSize;
    }

    public ServerOptions setMaxLogChunkSize(int maxLogChunkSize) {
        this.maxLogChunkSize = maxLogChunkSize;
        return this;
    }

    public int getMaxRecordSize() {
        return maxRecordSize;
    }

    public ServerOptions setMaxRecordSize(int maxRecordSize) {
        this.maxRecordSize = maxRecordSize;
        return this;
    }

    public int getPreallocateSize() {
        return preallocateSize;
    }

    public ServerOptions setPreallocateSize(int preallocateSize) {
        this.preallocateSize = preallocateSize;
        return this;
    }

    public int getReadBufferSize() {
        return readBufferSize;
    }

    public ServerOptions setReadBufferSize(int readBufferSize) {
        this.readBufferSize = readBufferSize;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ServerOptions that = (ServerOptions)o;

        if (maxLogChunkSize != that.maxLogChunkSize) return false;
        if (preallocateSize != that.preallocateSize) return false;
        if (maxRecordSize != that.maxRecordSize) return false;
        if (readBufferSize != that.readBufferSize) return false;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(channels, that.channels)) return false;
        if (netServerOptions != null ? !netServerOptions.equals(that.netServerOptions) : that.netServerOptions != null)
            return false;
        if (docsDir != null ? !docsDir.equals(that.docsDir) : that.docsDir != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(binders, that.binders);

    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(channels);
        result = 31 * result + (netServerOptions != null ? netServerOptions.hashCode() : 0);
        result = 31 * result + (docsDir != null ? docsDir.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(binders);
        result = 31 * result + maxLogChunkSize;
        result = 31 * result + preallocateSize;
        result = 31 * result + maxRecordSize;
        result = 31 * result + readBufferSize;
        return result;
    }
}
