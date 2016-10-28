package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.bson.BsonObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class AppendTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(AppendTest.class);

    @Test
    public void test_append(TestContext testContext) throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length();
        int numObjects = 100;
        options = getOptions().setMaxLogChunkSize(length * (numObjects + 1)).setMaxRecordSize(length + 1);
        startLog();
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        assertExists(0);
        assertLogChunkLength(0, obj.encode().length() * numObjects);
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects);
            BsonObject expected = obj.copy().put("num", cnt);
            assertTrue(expected.equals(record));
        });
    }

    @Test
    public void test_append_next_file(TestContext testContext) throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length();
        int numObjects = 100;
        options = getOptions().setMaxLogChunkSize(length * (numObjects - 1)).setMaxRecordSize(length + 1);
        startLog();
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        assertExists(0);
        assertLogChunkLength(0, options.getMaxLogChunkSize());

        AtomicInteger loadedCount = new AtomicInteger(0);
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects - 1);
            BsonObject expected = obj.copy().put("num", cnt);
            assertTrue(expected.equals(record));
            loadedCount.incrementAndGet();
        });

        assertExists(1);
        assertLogChunkLength(1, length);
        assertEquals(numObjects - 1, loadedCount.get());

        assertObjects(1, (cnt, record) -> {
            assertTrue(cnt < 1);
            BsonObject expected = obj.copy().put("num", 99);
            assertTrue(expected.equals(record));
        });
    }

    @Test
    public void test_append_concurrent(TestContext testContext) throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length();
        int numObjects = 100;
        options = getOptions().setMaxLogChunkSize(length * (numObjects + 1)).setMaxRecordSize(length + 1);
        startLog();
        appendObjectsConcurrently(numObjects, i -> obj.copy().put("num", i));
        assertExists(0);
        assertLogChunkLength(0, obj.encode().length() * numObjects);
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects);
            BsonObject expected = obj.copy().put("num", cnt);
            assertTrue(expected.equals(record));
        });
    }

    @Test
    public void test_prealloc(TestContext testContext) throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length();
        int numObjects = 100;
        int preallocSize = 10 * length;
        options = getOptions().setMaxLogChunkSize(length * (numObjects + 1)).setMaxRecordSize(length + 1).setPreallocateSize(preallocSize);
        startLog();
        assertExists(0);
        assertLogChunkLength(0, preallocSize);
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        assertObjects(0, (cnt, record) -> {
            assertTrue(cnt < numObjects);
            BsonObject expected = obj.copy().put("num", cnt);
            assertTrue(expected.equals(record));
        });
        assertLogChunkLength(0, obj.encode().length() * numObjects);
    }

    @Test
    public void test_prealloc_next_file(TestContext testContext) throws Exception {
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int length = obj.encode().length();
        int numObjects = 100;
        int preallocSize = 10 * length;
        options = getOptions().setMaxLogChunkSize(length * (numObjects + 1)).setMaxRecordSize(length + 1).setPreallocateSize(preallocSize);
        startLog();
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        assertExists(1);
        assertLogChunkLength(1, preallocSize);
    }


    protected void assertObjects(int fileNumber, BiConsumer<Integer, BsonObject> objectConsumer) throws Exception {
        File file = new File(logDir, getLogFileName(TEST_CHANNEL_1, fileNumber));
        assertTrue(file.exists());
        Buffer buff = readFileIntoBuffer(file);
        int pos = 0;
        int count = 0;
        while (true) {
            int objLen = buff.getIntLE(pos);
            if (objLen == 0) {
                break;
            }
            Buffer objBuff = buff.slice(pos, pos + objLen);
            BsonObject record = new BsonObject(objBuff);
            objectConsumer.accept(count, record);
            count++;
            pos += objLen;
            if (pos >= file.length()) {
                break;
            }
        }
    }


}
