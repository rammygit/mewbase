package com.tesco.mewbase.doc;

import com.tesco.mewbase.MewbaseTestBase;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.doc.impl.lmdb.LmdbDocManager;
import com.tesco.mewbase.log.impl.file.InitialiseTest;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by tim on 14/10/16.
 */
@RunWith(VertxUnitRunner.class)
public abstract class DocManagerTest extends MewbaseTestBase {

    private final static Logger logger = LoggerFactory.getLogger(DocManagerTest.class);

    protected DocManager docManager;
    protected File docsDir;
    protected DocReadStream stream;

    @Override
    protected void setup(TestContext context) throws Exception {
        super.setup(context);
        docsDir = testFolder.newFolder();
        docManager = createDocManager();
        docManager.start().get();
        createBinder(TEST_BINDER1);
        createBinder(TEST_BINDER2);
    }

    @Override
    protected void tearDown(TestContext context) throws Exception {
        if (stream != null) {
            stream.close();
        }
        docManager.close().get();
    }

    protected abstract DocManager createDocManager();

    @Test
    public void testSimplePutGet() throws Exception {
        BsonObject docPut = createObject();
        assertNull(docManager.put(TEST_BINDER1, "id1234", docPut).get());
        BsonObject docGet = docManager.get(TEST_BINDER1, "id1234").get();
        assertEquals(docPut, docGet);
    }

    @Test
    public void testFindNoEntry() throws Exception {
        assertNull(docManager.get(TEST_BINDER1, "id1234").get());
    }

    @Test
    public void testDelete() throws Exception {
        BsonObject docPut = createObject();
        assertNull(docManager.put(TEST_BINDER1, "id1234", docPut).get());
        BsonObject docGet = docManager.get(TEST_BINDER1, "id1234").get();
        assertEquals(docPut, docGet);
        assertTrue(docManager.delete(TEST_BINDER1, "id1234").get());
        docGet = docManager.get(TEST_BINDER1, "id1234").get();
        assertNull(docGet);
    }

    @Test
    public void testPutGetMultiple() throws Exception {
        int numDocs = 10;
        int numBinders = 10;
        for (int i = 0; i < numBinders; i++) {
            String binder = "pgmbinder" + i;
            createBinder(binder);
            for (int j = 0; j < numDocs; j++) {
                BsonObject docPut = createObject();
                docPut.put("binder", binder);
                assertNull(docManager.put(binder, "id" + j, docPut).get());
            }
        }
        for (int i = 0; i < numBinders; i++) {
            String binder = "pgmbinder" + i;
            for (int j = 0; j < numDocs; j++) {
                BsonObject docGet = docManager.get(binder, "id" + j).get();
                assertEquals(binder, docGet.remove("binder"));
            }
        }
    }

    @Test
    public void testRestart() throws Exception {
        BsonObject docPut = createObject();
        assertNull(docManager.put(TEST_BINDER1, "id1234", docPut).get());
        BsonObject docGet = docManager.get(TEST_BINDER1, "id1234").get();
        assertEquals(docPut, docGet);

        docManager.close().get();
        docManager = createDocManager();
        docManager.start().get();
        createBinder(TEST_BINDER1);

        docGet = docManager.get(TEST_BINDER1, "id1234").get();
        assertEquals(docPut, docGet);
    }

    @Test
    public void testStream(TestContext testContext) throws Exception {

        // Add some docs
        int numDocs = 100;
        addDocs(TEST_BINDER1, numDocs);

        // Add some docs in another binder
        addDocs(TEST_BINDER2, numDocs);

        stream = docManager.getMatching(TEST_BINDER1, doc -> true);

        Async async = testContext.async();

        AtomicInteger docCount = new AtomicInteger();
        stream.handler(doc -> {
            int expectedNum = docCount.getAndIncrement();
            int docNum = doc.getInteger("docNum");
            assertEquals(expectedNum, docNum);
            if (expectedNum == numDocs - 1) {
                async.complete();
            }
        });

        stream.start();
    }

    @Test
    public void testStreamWithFilter(TestContext testContext) throws Exception {

        // Add some docs
        int numDocs = 100;
        addDocs(TEST_BINDER1, numDocs);

        // Add some docs in another binder
        addDocs(TEST_BINDER2, numDocs);

        stream = docManager.getMatching(TEST_BINDER1, doc -> {
            int docNum = doc.getInteger("docNum");
            return docNum >= 10 && docNum < 90;
        });

        Async async = testContext.async();

        AtomicInteger docCount = new AtomicInteger(10);
        stream.handler(doc -> {
            int expectedNum = docCount.getAndIncrement();
            int docNum = doc.getInteger("docNum");
            assertEquals(expectedNum, docNum);
            if (expectedNum == 89) {
                async.complete();
            }
        });

        stream.start();
    }

    @Test
    public void testStreamPauseResume(TestContext testContext) throws Exception {

        // Add some docs
        int numDocs = 100;
        addDocs(TEST_BINDER1, numDocs);

        // Add some docs in another binder
        addDocs(TEST_BINDER2, numDocs);

        stream = docManager.getMatching(TEST_BINDER1, doc -> true);

        Async async = testContext.async();

        AtomicInteger docCount = new AtomicInteger();
        AtomicBoolean paused = new AtomicBoolean();
        stream.handler(doc -> {
            testContext.assertFalse(paused.get());
            int expectedNum = docCount.getAndIncrement();
            int docNum = doc.getInteger("docNum");
            assertEquals(expectedNum, docNum);

            if (expectedNum == numDocs / 2) {
                stream.pause();
                paused.set(true);
                vertx.setTimer(100, tid -> {
                    paused.set(false);
                    stream.resume();
                });
            }
            if (expectedNum == numDocs - 1) {
                async.complete();
            }
        });

        stream.start();
    }

    private void addDocs(String binderName, int numDocs) throws Exception {
        for (int i = 0; i < numDocs; i++) {
            BsonObject docPut = createObject();
            docPut.put("docNum", i);
            assertNull(docManager.put(binderName, getID(i), docPut).get());
        }
    }

    private String getID(int id) {
        return String.format("id-%05d", id);
    }

    protected BsonObject createObject() {
        BsonObject obj = new BsonObject();
        obj.put("foo", "bar").put("quux", 1234).put("wib", true);
        return obj;
    }

    protected void createBinder(String binderName) throws Exception {
        docManager.createBinder(binderName).get();
    }

}
