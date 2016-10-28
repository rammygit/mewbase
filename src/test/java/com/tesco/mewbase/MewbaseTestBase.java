package com.tesco.mewbase;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.impl.file.FileAccess;
import com.tesco.mewbase.log.impl.file.FileLogManager;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.log.impl.file.faf.AFFileAccess;
import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RepeatRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tim on 14/10/16.
 */
public class MewbaseTestBase {

    protected static final String TEST_CHANNEL_1 = "channel1";
    protected static final String TEST_CHANNEL_2 = "channel2";

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    @Rule
    public RepeatRule repeatRule = new RepeatRule();


    protected Vertx vertx;

    @Before
    public void before(TestContext context) throws Exception {
        setup(context);
    }

    protected void setup(TestContext context) throws Exception {
        vertx = Vertx.vertx();
    }

    @After
    public void after(TestContext context) throws Exception {
        tearDown(context);
    }

    protected void tearDown(TestContext context) throws Exception {
        Async async = context.async();
        vertx.close(ar -> {
            if (ar.succeeded()) {
                async.complete();
            } else {
                context.fail(ar.cause());
            }
        });
    }

    protected void waitUntil(BooleanSupplier supplier) {
        waitUntil(supplier, 10000);
    }

    protected void waitUntil(BooleanSupplier supplier, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            if (supplier.getAsBoolean()) {
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException ignore) {
            }
            long now = System.currentTimeMillis();
            if (now - start > timeout) {
                throw new IllegalStateException("Timed out");
            }
        }
    }

}
