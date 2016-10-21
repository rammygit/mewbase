package com.tesco.mewbase.log.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.log.Log;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by tim on 27/09/16.
 */
public class InMemoryLog implements Log {

    private final static Logger logger = LoggerFactory.getLogger(InMemoryLog.class);

    private final Buffer log = Buffer.buffer();
    private long headPos;

    public CompletableFuture<Long> append(BsonObject obj) {
        Buffer buff = obj.encode();
        log.appendBuffer(buff);
        long pos = headPos;
        headPos += buff.length();
        CompletableFuture<Long> cf = new CompletableFuture<>();
        cf.complete(pos);
        logger.trace("Appended obj {} at pos {}", obj, pos);
        return cf;
    }

    @Override
    public CompletableFuture<Void> start() {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cf.complete(null);
        return cf;
    }

    @Override
    public CompletableFuture<Void> close() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ReadStream> openReadStream(long pos) {
        CompletableFuture<ReadStream> cf = new CompletableFuture<>();
        cf.complete(new InMemoryReadStream(new ElemIterator(log, (int)pos)));
        return cf;
    }

    @Override
    public synchronized int getFileNumber() {
        return 0;
    }

    @Override
    public synchronized long getHeadPos() {
        return headPos;
    }

    @Override
    public synchronized int getFilePos() {
        return (int)headPos;
    }

    private static final class ElemIterator implements Iterator<QueueEntry> {

        private final Buffer log;
        private int pos;

        public ElemIterator(Buffer log, int pos) {
            this.log = log;
            this.pos = pos;
        }

        @Override
        public boolean hasNext() {
            return pos != log.length();
        }

        @Override
        public QueueEntry next() {
            int size = log.getIntLE(pos);
            Buffer buff = log.slice(pos, pos + size);
            BsonObject obj = new BsonObject(buff);
            QueueEntry entry = new QueueEntry(pos, obj);
            pos += size;
            return entry;
        }
    }

}
