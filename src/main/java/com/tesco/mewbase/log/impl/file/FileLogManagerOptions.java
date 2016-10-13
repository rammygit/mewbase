package com.tesco.mewbase.log.impl.file;

/**
 * Created by tim on 08/10/16.
 */
public class FileLogManagerOptions {

    public static final String DEFAULT_LOG_DIR = "mewlog";
    public static final int DEFAULT_FILE_SIZE = 4 * 10 * 1024 * 1024;
    public static final int DEFAULT_PREALLOCATE_NUMBER = 2;

    private String logDir = DEFAULT_LOG_DIR;
    private int fileSize = DEFAULT_FILE_SIZE;
    private int preallocateNumber = DEFAULT_PREALLOCATE_NUMBER;

    public String getLogDir() {
        return logDir;
    }

    public FileLogManagerOptions setLogDir(String logDir) {
        this.logDir = logDir;
        return this;
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileLogManagerOptions setFileSize(int fileSize) {
        this.fileSize = fileSize;
        return this;
    }

    public int getPreallocateNumber() {
        return preallocateNumber;
    }

    public FileLogManagerOptions setPreallocateNumber(int preallocateNumber) {
        this.preallocateNumber = preallocateNumber;
        return this;
    }
}
