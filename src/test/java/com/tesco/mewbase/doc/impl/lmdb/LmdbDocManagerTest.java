package com.tesco.mewbase.doc.impl.lmdb;

import com.tesco.mewbase.MewbaseTestBase;
import com.tesco.mewbase.bson.Bson;
import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.doc.DocManagerTest;
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
import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

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
@RunWith(VertxUnitRunner.class)
public class LmdbDocManagerTest extends DocManagerTest {

    protected DocManager createDocManager() {
        return new LmdbDocManager(docsDir.getPath(), vertx);
    }

    //@Repeat(value = 1000)
    @Override
    @Test
    public void testStream(TestContext testContext) throws Exception {
        super.testStream(testContext);
    }
}
