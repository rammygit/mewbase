package com.tesco.mubase.log.impl.inmem;

import com.tesco.mubase.bson.BsonObject;
import com.tesco.mubase.log.Log;
import com.tesco.mubase.log.LogReadStream;
import com.tesco.mubase.log.LogWriteStream;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by tim on 27/09/16.
 */
public class InMemoryLog implements Log {

    private final Queue<BsonObject> queue = new ConcurrentLinkedQueue<>();

    public CompletableFuture<Void> append(BsonObject obj) {
        queue.add(obj);
        CompletableFuture<Void> cf= new CompletableFuture<>();
        cf.complete(null);
        return cf;
    }

    @Override
    public LogReadStream openReadStream(long seqNumber) {
        return new InMemoryLogReadStream(seqNumber, queue.iterator());
    }

    @Override
    public LogWriteStream openWriteStream() {
        return new InMemoryLogWriteStream(queue);
    }
}
