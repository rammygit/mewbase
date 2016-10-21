package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class StreamTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(StreamTest.class);

    @Test
    public void test_stream(TestContext testContext) throws Exception {
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        int numObjects = 100;
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
        CompletableFuture<ReadStream> cf = log.openReadStream(0);
        ReadStream rs = cf.get();
        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        rs.handler((pos, record) -> {
            testContext.assertEquals("bar", record.getString("foo"));
            testContext.assertEquals(cnt.get(), record.getInteger("num"));
            if (cnt.incrementAndGet() == numObjects) {
                rs.close();
                async.complete();
            }
        });
        rs.resume();
    }

//    @Test
//    public void test_stream_multiple_files(TestContext testContext) throws Exception {
//
//        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
//        int length = obj.encode().length();
//        int numObjects = 100;
//        options = getOptions().setMaxLogChunkSize(length * (numObjects - 1)).setMaxRecordSize(length + 1);
//        startLog();
//        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
//        assertExists(0);
//        assertLogChunkLength(0, options.getMaxLogChunkSize());
//        assertExists(1);
//        assertLogChunkLength(1, length);
//
//        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));
//        CompletableFuture<ReadStream> cf = log.openReadStream(0);
//        ReadStream rs = cf.get();
//        Async async = testContext.async();
//        AtomicInteger cnt = new AtomicInteger();
//        rs.handler((pos, record) -> {
//            testContext.assertEquals("bar", record.getString("foo"));
//            testContext.assertEquals(cnt.get(), record.getInteger("num"));
//            if (cnt.incrementAndGet() == numObjects) {
//                rs.close();
//                async.complete();
//            }
//        });
//        rs.resume();
//    }


}
