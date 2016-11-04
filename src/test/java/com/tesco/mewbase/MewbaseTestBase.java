package com.tesco.mewbase;

import com.tesco.mewbase.util.AsyncResCF;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RepeatRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

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
        AsyncResCF<Void> cf = new AsyncResCF<>();
        vertx.close(cf);
        cf.get();
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

    protected <T> T waitForNonNull(Supplier<T> supplier) {
        return waitForNonNull(supplier, 10000);
    }

    protected <T> T waitForNonNull(Supplier<T> supplier, long timeout) {
        long start = System.currentTimeMillis();
        while (true) {
            T res = supplier.get();
            if (res != null) {
                return res;
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
