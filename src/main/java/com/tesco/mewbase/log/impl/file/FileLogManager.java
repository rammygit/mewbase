package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogManager;
import io.vertx.core.Vertx;

import java.io.File;

/**
 * Created by tim on 07/10/16.
 */
public class FileLogManager implements LogManager {

    private final Vertx vertx;
    private final FileAccessManager faf;
    private final File logDir;
    private final FileLogManagerOptions options;

    public FileLogManager(Vertx vertx, FileLogManagerOptions options, FileAccessManager faf) {
        this.vertx = vertx;
        this.logDir = new File(options.getLogDir());
        if (!logDir.exists()) {
            if (!logDir.mkdirs()) {
                throw new MewException("Failed to create directory " + options.getLogDir());
            }
        }
        this.options = options;
        this.faf = faf;
    }

    @Override
    public Log getLog(String channel) {
        return new FileLog(vertx, faf, options, channel);
    }
}
