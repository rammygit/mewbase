package com.tesco.mewbase.log.impl.inmem;

import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.LogManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tim on 27/09/16.
 */
public class InMemoryLogManager implements LogManager {

    private final Map<String, InMemoryLog> logs = new ConcurrentHashMap<>();

    @Override
    public Log getLog(String channel) {
        InMemoryLog log = logs.get(channel);
        if (log == null) {
            log = new InMemoryLog();
            InMemoryLog prev = logs.putIfAbsent(channel, log);
            if (prev != null) {
                log = prev;
            }
        }
        return log;
    }
}
