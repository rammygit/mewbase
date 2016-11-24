package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogManager;
import com.tesco.mewbase.server.impl.ServerImpl;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tim on 07/10/16.
 */
public class FileLogManager implements LogManager {

    private final static Logger log = LoggerFactory.getLogger(FileLogManager.class);

    private final Vertx vertx;
    private final FileAccess faf;
    private final File logDir;
    private final FileLogManagerOptions options;
    private final Map<String, FileLog> logs = new ConcurrentHashMap<>();

    public FileLogManager(Vertx vertx, FileLogManagerOptions options, FileAccess faf) {

        if (options.getLogDir().contains("mewlog")) {
            log.trace("****************************************** LOGS DIR");
            try {
                Thread.sleep(1000000);
            } catch (Exception e) {
            }
            System.exit(1);
        }
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
    public CompletableFuture<Log> createLog(String channel) {
        FileLog log = new FileLog(vertx, faf, options, channel);
        logs.put(channel, log);
        return log.start().thenApply(v -> log);
    }

    @Override
    public CompletableFuture<Void> close() {
        CompletableFuture[] arr = new CompletableFuture[logs.size()];
        int i = 0;
        for (Log log : logs.values()) {
            arr[i++] = log.close();
        }
        return CompletableFuture.allOf(arr);
    }

    @Override
    public Log getLog(String channel) {
        return logs.get(channel);
    }
}
