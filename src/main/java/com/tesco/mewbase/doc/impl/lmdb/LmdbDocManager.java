package com.tesco.mewbase.doc.impl.lmdb;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.doc.DocReadStream;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.server.impl.ConnectionImpl;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.buffer.Buffer;
import org.fusesource.lmdbjni.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * lmdb is fast but unfortunately has a blocking API so we have to execute everything on worker threads
 *
 * We use one lmdb database per binder
 *
 * Created by tim on 09/11/16.
 */
public class LmdbDocManager implements DocManager {

    private final static Logger logger = LoggerFactory.getLogger(LmdbDocManager.class);

    // TODO make configurable
    private static final String LMDB_DOCMANAGER_POOL_NAME = "mewbase.docmanagerpool";
    private static final int LMDB_DOCMANAGER_POOL_SIZE = 10;

    private final Vertx vertx;
    private final File docsDir;
    private final Map<String, DBHolder> databases = new ConcurrentHashMap<>();
    private final WorkerExecutor exec;

    public LmdbDocManager(String docsDir, Vertx vertx) {
        logger.trace("Starting lmdb docmanager with docs dir: " + docsDir);
        this.docsDir = new File(docsDir);
        this.vertx = vertx;
        exec = vertx.createSharedWorkerExecutor(LMDB_DOCMANAGER_POOL_NAME, LMDB_DOCMANAGER_POOL_SIZE);
    }

    @Override
    public CompletableFuture<BsonObject> get(String binderName, String id) {

        DBHolder holder = getDBHolder(binderName);
        AsyncResCF<BsonObject> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
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
        exec.executeBlocking(fut -> {
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
        exec.executeBlocking(fut -> {
            byte[] key = getKey(id);
            boolean deleted = holder.db.delete(key);
            fut.complete(deleted);
        }, res);
        return res;
    }

    public CompletableFuture<Void> close() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
            for (DBHolder holder: databases.values()) {
                holder.close();
            }
            exec.close();
            fut.complete(null);
        }, res);
        return res;
    }

    public CompletableFuture<Void> start() {
        AsyncResCF<Void> res = new AsyncResCF<>();
        exec.executeBlocking(fut -> {
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
        exec.executeBlocking(fut -> {
            File dbDir = new File(docsDir, "binder-" + binderName);
            createIfDoesntExists(dbDir);
            Env env = new Env();
            env.open(dbDir.getPath(), Constants.NOTLS);
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

        private static final int MAX_DELIVER_BATCH = 100;

        private final Transaction tx;
        private final EntryIterator iter;
        private final Function<BsonObject, Boolean> matcher;
        private Consumer<BsonObject> handler;
        private boolean paused;
        private boolean hasMore;
        private boolean handledOne;
        private boolean closed;

        LmdbReadStream(DBHolder holder, Function<BsonObject, Boolean> matcher) {
            this.tx = holder.env.createReadTransaction();
            this.iter = holder.db.iterate(tx);
            this.matcher = matcher;
        }

        @Override
        public void exceptionHandler(Consumer<Throwable> handler) {
        }

        @Override
        public void handler(Consumer<BsonObject> handler) {
            this.handler = handler;
        }

        @Override
        public synchronized void start() {
            printThread();
            iterNext();
        }

        @Override
        public synchronized void pause() {
            printThread();
            paused = true;
        }

        @Override
        public synchronized void resume() {
            printThread();
            paused = false;
            iterNext();
        }

        @Override
        public synchronized void close() {
            printThread();
            if (!closed) {
                iter.close();
                // Beware calling tx.close() if the database/env object is closed can cause a core dump:
                // https://github.com/deephacks/lmdbjni/issues/78
                tx.close();
                closed = true;
            }
        }

        public synchronized boolean hasMore() {
            return hasMore;
        }

        private void printThread() {
            //logger.trace("Thread is {}", Thread.currentThread());
        }

        private synchronized void iterNext() {
            printThread();
            if (paused || closed) {
                return;
            }
            for (int i = 0; i < MAX_DELIVER_BATCH; i++) {
                if (iter.hasNext()) {
                    Entry entry = iter.next();

                    byte[] val = entry.getValue();
                    BsonObject doc = new BsonObject(Buffer.buffer(val));
                    if (handler != null && matcher.apply(doc)) {
                        hasMore = iter.hasNext();
                        handler.accept(doc);
                        handledOne = true;
                        if (paused) {
                            return;
                        }
                    }
                } else {
                    if (!handledOne) {
                        // Send back an empty result
                        handler.accept(null);
                    }
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
        boolean closed;

        DBHolder(Env env, Database db) {
            this.env = env;
            this.db = db;
        }

        void close() {
            db.close();
            env.close();
            closed = true;
        }
    }

}
