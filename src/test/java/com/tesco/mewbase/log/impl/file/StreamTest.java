package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.common.SubDescriptor;
import com.tesco.mewbase.log.impl.file.faf.af.AsyncFileFileAccessManager;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class StreamTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(StreamTest.class);

//    @Test
//    public void test_stream_defaults(TestContext testContext) throws Exception {
//        test_stream(testContext, 100, FileLogManagerOptions.DEFAULT_MAX_LOG_CHUNK_SIZE,
//                FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE, FileLogManagerOptions.DEFAULT_MAX_RECORD_SIZE);
//    }

    /*

    Test retro:

    Test no prealloc:

    all in one file (much less than max file chunk size)
    all in one file (exactly equal to mfcs)
    all in one file (little bit of empty space at end of file)

    two files - filling first file exactly
    two files - filling first file with some empty space at end
    two files - filling two files exactly
    two files - filling both files with some space at end

    three files

    test pause/resume

    test start at non zero pos

    test start at -ve pos -> fail
    test start at too big pos -> fail
    test start at invalid pos (middle of object) -> fail


    test active, position pos at head and assert new messages are received

    test move from retro to active

    test pause/resume when active - no msgs delivered in interim
    test pause/resume when active - msgs delivered in interim - switch back to retro then catch up again


    test multiple subs on same channel

     */

    private BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
    private int objLen = obj.encode().length();
    private int numObjects = 100;

    @Test
    public void test_stream_single_file_less_than_max_file_size(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects + 10);
        test_stream(testContext, numObjects, fileSize, FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                          0, numObjects * objLen, objLen);
    }

    @Test
    public void test_stream_single_file_equal_to_max_file_size(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects;
        test_stream(testContext, numObjects, fileSize, FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                0, numObjects * objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_first_exactly(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects - 1);
        test_stream(testContext, numObjects, fileSize, FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                1, objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_first_with_empty_space(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects - 1) + (objLen / 2);
        test_stream(testContext, numObjects, fileSize, FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                1, objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_both_exactly(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects  / 2;
        test_stream(testContext, numObjects, fileSize, FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                1, fileSize, objLen);
    }

    @Test
    public void test_stream_five_files_with_empty_space(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects / 5) + (objLen / 2);
        test_stream(testContext, numObjects, fileSize, FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                4, objLen * (numObjects / 5), objLen);
    }

    @Test
    public void test_stream_five_files_fill_both_exactly(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects / 5;
        test_stream(testContext, numObjects, fileSize, FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE, objLen,
                4, fileSize, objLen);
    }

    @Test
    public void test_stream_single_file_less_than_max_file_size_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects + 10);
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                0, numObjects * objLen, objLen);
    }

    @Test
    public void test_stream_single_file_equal_to_max_file_size_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                0, numObjects * objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_first_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects - 1);
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                1, objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_first_with_empty_space_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects - 1) + (objLen / 2);
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                1, objLen, objLen);
    }

    @Test
    public void test_stream_two_files_fill_both_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects  / 2;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                1, fileSize, objLen);
    }

    @Test
    public void test_stream_five_files_with_empty_space_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * (numObjects / 5) + (objLen / 2);
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                4, objLen * (numObjects / 5), objLen);
    }

    @Test
    public void test_stream_five_files_fill_both_exactly_small_rb(TestContext testContext) throws Exception {
        int fileSize = objLen * numObjects / 5;
        test_stream(testContext, numObjects, fileSize, objLen - 1, objLen,
                4, fileSize, objLen);
    }

    protected void test_stream(TestContext testContext, int numObjects, int maxLogChunkSize, int readBuffersize,
                               int maxRecordSize, int expectedEndFile, int expectedEndFileLength, int objLen)
            throws Exception {
        options = new FileLogManagerOptions().setMaxLogChunkSize(maxLogChunkSize).
                setReadBufferSize(readBuffersize).setMaxRecordSize(maxRecordSize).setLogDir(logDir.getPath());
        startLog();

        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        ReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1));
        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        rs.handler((pos, record) -> {
            testContext.assertEquals("bar", record.getString("foo"));
            testContext.assertEquals(cnt.get(), record.getInteger("num"));
            long expectedPos = calcPos(cnt.get(), maxLogChunkSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
            if (cnt.incrementAndGet() == numObjects) {
                rs.close();
                testContext.assertEquals(expectedEndFile, log.getFileNumber());
                // Check the lengths of the files
                File[] files = super.listLogFiles(logDir, TEST_CHANNEL_1);
                String headFileName = getLogFileName(TEST_CHANNEL_1, log.getFileNumber());
                String preallocedFileName = getLogFileName(TEST_CHANNEL_1, log.getFileNumber() + 1);
                for (File f: files) {
                    String fname = f.getName();
                    if (fname.equals(headFileName)) {
                        testContext.assertEquals((long)expectedEndFileLength, f.length());
                    } else if (!fname.equals(preallocedFileName)) {
                        testContext.assertEquals((long)maxLogChunkSize, f.length());
                    }
                }
                async.complete();
            }
        });
    }

    @Test
    public void test_pause_resume_in_retro(TestContext testContext) throws Exception {
        int fileSize = objLen * 20;
        options = new FileLogManagerOptions().setMaxLogChunkSize(fileSize).
                setReadBufferSize(FileLogManagerOptions.DEFAULT_READ_BUFFER_SIZE).setMaxRecordSize(objLen).setLogDir(logDir.getPath());
        startLog();
        BsonObject obj = new BsonObject().put("foo", "bar").put("num", 0);
        appendObjectsSequentially(numObjects, i -> obj.copy().put("num", i));

        ReadStream rs = log.subscribe(new SubDescriptor().setChannel(TEST_CHANNEL_1));
        Async async = testContext.async();
        AtomicInteger cnt = new AtomicInteger();
        AtomicBoolean paused = new AtomicBoolean();
        rs.handler((pos, record) -> {
            testContext.assertFalse(paused.get());
            testContext.assertEquals("bar", record.getString("foo"));
            testContext.assertEquals(cnt.get(), record.getInteger("num"));
            long expectedPos = calcPos(cnt.get(), fileSize, objLen);
            testContext.assertEquals(expectedPos, (long)pos);
            if (cnt.incrementAndGet() == numObjects) {
                rs.close();
                async.complete();
            }
            // Pause every 5 msgs
            if (cnt.get() % 5 == 0) {
                rs.pause();
                paused.set(true);
                vertx.setTimer(10, tid -> {
                   paused.set(false);
                    rs.resume();
                });
            }
        });
    }

    /*
    Calculate the apend position of the nth object to be appended to the log, n starts at zero
     */
    protected long calcPos(int nth, int maxLogChunkSize, int objLength) {

        logger.trace("calculating pos for n {}  mlcs {} ol {}", nth, maxLogChunkSize, objLength);

        int pos = 0;
        int filePos = 0;

        for (int i = 0; i < nth; i++) {
            pos += objLength;
            filePos += objLength;
            int remainingSpace = maxLogChunkSize - filePos;
            if (remainingSpace < objLength) {
                pos += remainingSpace;
                filePos = 0;
            }
        }

        logger.trace("Pos is {}", pos);


        return pos;
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
