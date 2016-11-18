package com.tesco.mewbase.doc.impl.lmdb;

import com.tesco.mewbase.doc.DocManager;
import com.tesco.mewbase.doc.DocManagerTest;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.runner.RunWith;

/**
 * Created by tim on 14/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class LmdbDocManagerTest extends DocManagerTest {

    protected DocManager createDocManager() {
        return new LmdbDocManager(docsDir.getPath(), vertx);
    }

}
