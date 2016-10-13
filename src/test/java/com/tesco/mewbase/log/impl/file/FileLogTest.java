package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.impl.file.faf.af.AsyncFileFileAccessManager;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class FileLogTest {

    private final static Logger logger = LoggerFactory.getLogger(FileLogTest.class);

    private static final String TEST_CHANNEL = "com.tesco.basket";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private Vertx vertx;
    private Log log;

    @Before
    public void before(TestContext context) throws Exception {
        File ftestDir = testFolder.newFolder();
        logger.trace("test dir is {}", ftestDir);
        vertx = Vertx.vertx();
        FileAccessManager faf = new AsyncFileFileAccessManager(vertx);
        FileLogManagerOptions options = new FileLogManagerOptions().setLogDir(ftestDir.getPath());
        FileLogManager flm = new FileLogManager(vertx, options, faf);
        log = flm.getLog(TEST_CHANNEL);
        log.start().get();
    }

    @After
    public void after(TestContext context) throws Exception {
        log.close();
        vertx.close();
    }

    @Test
    public void testAppend(TestContext context) throws Exception {
        Async async = context.async(10);
        for (int i = 0; i < 10; i++) {
            BsonObject obj = new BsonObject();
            obj.put("id", i);
            obj.put("foo", "bar");
            CompletableFuture<Long> cf = log.append(obj);
            cf.handle((pos, t) -> {
                if (t == null) {
                    logger.trace("Completed with pos {}", pos);
                } else {
                    context.fail(t);
                }
                async.complete();
                return null;
            });
        }
    }

    @Test
    public void testReadStream(TestContext context) throws Exception {
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            BsonObject obj = new BsonObject();
            obj.put("id", i);
            obj.put("foo", "bar");
            CompletableFuture<Long> cf = log.append(obj);
            cf.handle((pos, t) -> {
                if (t == null) {
                    logger.trace("Completed with pos {}", pos);
                } else {
                    context.fail(t);
                }
                latch.countDown();
                return null;
            });
        }

        latch.await(5, TimeUnit.SECONDS);

        ReadStream rs = log.openReadStream(0).get();
        AtomicInteger cnt = new AtomicInteger();
        CountDownLatch latch2 = new CountDownLatch(10);
        rs.handler((pos, bson) -> {
            int id = bson.getInteger("id");
            logger.trace("Got msg {}", id);
            logger.trace("Thread is {}", Thread.currentThread());
            context.assertEquals(cnt.getAndIncrement(), id);
            context.assertEquals("bar", bson.getString("foo"));
            latch2.countDown();
        });
        rs.resume();

        latch2.await(5, TimeUnit.SECONDS);
        rs.close();
    }

}
