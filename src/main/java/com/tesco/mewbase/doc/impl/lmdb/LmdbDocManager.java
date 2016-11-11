package com.tesco.mewbase.doc.impl.lmdb;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.doc.DocReadStream;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.fusesource.lmdbjni.*;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 * DocManager using lmdb.
 *
 * (I don't like the blocking API!)
 *
 * Created by tim on 09/11/16.
 */
public class LmdbDocManager implements DocManager {

    private final Vertx vertx;
    private final File docsDir;
    private final Map<String, DBHolder> databases = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<BsonObject> get(String binderName, String id) {
        DBHolder holder = getDBHolder(binderName);
        AsyncResCF<BsonObject> res = new AsyncResCF<>();
        vertx.executeBlocking(fut -> {
            byte[] key = getKey(id);
            byte[] val = holder.db.get(key);
            if (val != null) {
                BsonObject obj = new BsonObject(Buffer.buffer(val));
                fut.complete(obj);
            } else {
                fut.complete(null);
            }
        }, res);
        return res;
    }

    @Override
    public DocReadStream getMatching(String binderName, Function<BsonObject, Boolean> matcher) {
        DBHolder holder = getDBHolder(binderName);
        return new LmdbReadStream(holder, matcher);
    }

    @Override
    public CompletableFuture<Void> put(String binderName, String id, BsonObject doc) {
        DBHolder holder = getDBHolder(binderName);
        AsyncResCF<Void> res = new AsyncResCF<>();
        vertx.executeBlocking(fut -> {
            byte[] key = getKey(id);
            byte[] val = doc.encode().getBytes();
            holder.db.put(key, val);
            fut.complete(null);
        }, res);
        return res;
    }

    @Override
    public CompletableFuture<Boolean> delete(String binderName, String id) {
        DBHolder holder = getDBHolder(binderName);
        AsyncResCF<Boolean> res = new AsyncResCF<>();
        vertx.executeBlocking(fut -> {
            byte[] key = getKey(id);
            boolean deleted = holder.db.delete(key);
            fut.complete(deleted);
        }, res);
        return res;
    }

    public LmdbDocManager(String docsDir, Vertx vertx) {
        this.docsDir = new File(docsDir);
        this.vertx = vertx;
    }

    public CompletableFuture<Void> close() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        vertx.executeBlocking(fut -> {
            for (DBHolder holder: databases.values()) {
                holder.db.close();
                holder.env.close();
            }
            fut.complete(null);
        }, res);
        return res;
    }

    public CompletableFuture<Void> start() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        vertx.executeBlocking(fut -> {
            createIfDoesntExists(docsDir);
            fut.complete(null);
        }, res);
        return res;
    }

    @Override
    public CompletableFuture<Void> createBinder(String binderName) {
        if (databases.containsKey(binderName)) {
            throw new MewException("Already a binder called " + binderName);
        }
        AsyncResCF<Void> res = new AsyncResCF<>();
        vertx.executeBlocking(fut -> {
            File dbDir = new File(docsDir, "binder-" + binderName);
            createIfDoesntExists(dbDir);
            Env env = new Env(dbDir.getPath());
            Database db = env.openDatabase();
            databases.put(binderName, new DBHolder(env, db));
            fut.complete(null);
        }, res);
        return res;
    }

    private void createIfDoesntExists(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new MewException("Failed to create dir " + dir);
            }
        }
    }

    private byte[] getKey(String id) {
        // TODO probably a better way to do this
        return id.getBytes(StandardCharsets.UTF_8);
    }

    private DBHolder getDBHolder(String binderName) {
        DBHolder holder = databases.get(binderName);
        if (holder == null) {
            throw new MewException("No such binder " + binderName);
        }
        return holder;
    }

    private class LmdbReadStream implements DocReadStream {

        private Transaction tx;
        private EntryIterator iter;
        private Consumer<BsonObject> handler;
        private Consumer<Throwable> exceptionHandler;
        private boolean paused;
        private boolean closed;
        private Function<BsonObject, Boolean> matcher;

        LmdbReadStream(DBHolder holder, Function<BsonObject, Boolean> matcher) {
            this.tx = holder.env.createReadTransaction();
            this.iter = holder.db.iterate(tx);
            this.matcher = matcher;
        }

        @Override
        public void exceptionHandler(Consumer<Throwable> handler) {
            this.exceptionHandler = handler;
        }

        @Override
        public void handler(Consumer<BsonObject> handler) {
            this.handler = handler;
        }

        @Override
        public synchronized void start() {
            iterNext();
        }

        @Override
        public synchronized void pause() {
            paused = true;
        }

        @Override
        public synchronized void resume() {
            paused = false;
            iterNext();
        }

        @Override
        public synchronized void close() {
            closed = true;
            iter.close();
            tx.close();
        }

        public synchronized boolean isClosed() {
            return closed;
        }

        private static final int MAX_DELIVER_BATCH = 100;

        private void iterNext() {
            if (paused) {
                return;
            }
            for (int i = 0; i < MAX_DELIVER_BATCH; i++) {
                if (iter.hasNext()) {
                    Entry entry = iter.next();
                    byte[] val = entry.getValue();
                    BsonObject doc = new BsonObject(Buffer.buffer(val));
                    if (handler != null && matcher.apply(doc)) {
                        handler.accept(doc);
                        if (paused) {
                            return;
                        }
                    }
                } else {
                    close();
                    return;
                }
            }
            vertx.runOnContext(v -> iterNext());
        }
    }

    private static class DBHolder {
        final Env env;
        final Database db;

        public DBHolder(Env env, Database db) {
            this.env = env;
            this.db = db;
        }
    }



}
