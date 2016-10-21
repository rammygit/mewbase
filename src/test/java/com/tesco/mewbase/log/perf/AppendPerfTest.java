package com.tesco.mewbase.log.perf;

import com.tesco.mewbase.log.Log;
import com.tesco.mewbase.log.impl.file.AppendTest;
import com.tesco.mewbase.log.impl.file.FileAccessManager;
import com.tesco.mewbase.log.impl.file.FileLogManager;
import com.tesco.mewbase.log.impl.file.FileLogManagerOptions;
import com.tesco.mewbase.log.impl.file.faf.af.AsyncFileFileAccessManager;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * Manual test to compare performance of:
 *
 * 1. Not pre-allocating file
 * 2. Pre-allocating single file in chunks
 * 3. Pre-allocating multiple files
 *
 * Created by tim on 15/10/16.
 */
public class AppendPerfTest {

    private final static Logger logger = LoggerFactory.getLogger(AppendPerfTest.class);

    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    protected Vertx vertx;
    protected Log log;
    protected File testDir;

    @Before
    public void before() throws Exception {
        testDir = testFolder.newFolder();
        vertx = Vertx.vertx();
    }

//    @Test
//    public void testNoPrealloc() throws Exception {
//        File testFile = new File(testDir, "no-prealloc.dat");
//
//        logger.trace("Test file is {}", testFile);
//
//        assertFalse(testFile.exists());
//        assertTrue(testFile.createNewFile());
//
//        int chunkSize = 1024;
//        long maxFileSize = 10L * 1024 * 1024 * 1024;
//        long syncSize = 10 * 1024 * 1024;
//        long size = 0;
//
//        RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
//        byte[] bytes = new byte[chunkSize];
//
//        Arrays.fill(bytes, 0, bytes.length, (byte)'X');
//        //ByteBuffer buff = ByteBuffer.wrap(bytes);
//
//        long start = System.currentTimeMillis();
//
//        while (size < maxFileSize) {
//            raf.write(bytes);
//            size += chunkSize;
//            if (size % syncSize == 0) {
//                raf.getFD().sync();
//            }
//        }
//
//        long end = System.currentTimeMillis();
//
//        long dur = end - start;
//
//        double rate = 1000 * (double)testFile.length() / (dur * 1024 * 1024);
//
//        logger.trace("That took {} ms rate is {} MB/sec", dur, rate);
//
//    }
//
//    @Test
//    public void testWithPrealloc() throws Exception {
//        File testFile = new File(testDir, "no-prealloc.dat");
//
//        logger.trace("Test file is {}", testFile);
//
//        assertFalse(testFile.exists());
//        assertTrue(testFile.createNewFile());
//
//        int preallocSize = 100 * 1024 * 1024;
//        byte[] preallocBytes = new byte[preallocSize];
//        Arrays.fill(preallocBytes, 0, preallocBytes.length, (byte)'A');
//
//        int chunkSize = 1024;
//        long maxFileSize = 10L * 1024 * 1024 * 1024;
//        long syncSize = 10 * 1024 * 1024;
//        long size = 0;
//
//        long preSize = 0;
//
//        RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
//        byte[] bytes = new byte[chunkSize];
//
//        Arrays.fill(bytes, 0, bytes.length, (byte)'X');
//        //ByteBuffer buff = ByteBuffer.wrap(bytes);
//
//        long start = System.currentTimeMillis();
//
//        while (size < maxFileSize) {
//
//            if (size >= preSize) {
//                long currPos = raf.getFilePointer();
//                raf.write(preallocBytes);
//                preSize += preallocSize;
//                raf.seek(currPos);
//                logger.trace("Preallocating from {}", currPos);
//            }
//
//            raf.write(bytes);
//            size += chunkSize;
//            if (size % syncSize == 0) {
//                raf.getFD().sync();
//            }
//        }
//
//        long end = System.currentTimeMillis();
//
//        long dur = end - start;
//
//        double rate = 1000 * (double)testFile.length() / (dur * 1024 * 1024);
//
//        logger.trace("That took {} ms rate is {} MB/sec", dur, rate);
//
//    }

    @Test
    public void testFoo() {

    }
}
