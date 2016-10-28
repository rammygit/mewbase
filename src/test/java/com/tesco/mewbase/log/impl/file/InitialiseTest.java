package com.tesco.mewbase.log.impl.file;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.log.Log;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.UUID;

import static com.tesco.mewbase.log.impl.file.FileLogManagerOptions.DEFAULT_MAX_LOG_CHUNK_SIZE;
import static org.junit.Assert.*;

/**
 * Created by tim on 08/10/16.
 */
@RunWith(VertxUnitRunner.class)
public class InitialiseTest extends LogTestBase {

    private final static Logger logger = LoggerFactory.getLogger(InitialiseTest.class);

    @Test
    public void test_when_starting_log_dir_is_created(TestContext testContext) throws Exception {
        File ftestDir = testFolder.newFolder();
        String subDir = UUID.randomUUID().toString();
        File dir = new File(ftestDir, subDir);
        assertFalse(dir.exists());
        options = new FileLogManagerOptions().setLogDir(dir.getPath());
        startLog(options, TEST_CHANNEL_1);
        assertTrue(dir.exists());
    }

    @Test
    public void test_when_starting_with_existing_log_dir_new_files_are_created(TestContext testContext) throws Exception {
        startLog();
        verifyInitialFiles(logDir, TEST_CHANNEL_1);
    }

    @Test
    public void test_when_starting_and_non_zero_preallocate_size(TestContext testContext) throws Exception {
        options = getOptions().setPreallocateSize(1024 * 1024);
        startLog();
        verifyInitialFiles(logDir, TEST_CHANNEL_1);
    }

    @Test
    public void test_when_starting_without_existing_log_dir_new_files_are_created(TestContext testContext) throws Exception {
        File ftestDir = testFolder.newFolder();
        String subDir = UUID.randomUUID().toString();
        File ld = new File(ftestDir, subDir);
        assertFalse(ld.exists());
        logDir = ld;
        options = new FileLogManagerOptions().setLogDir(ld.getPath());
        startLog(options, TEST_CHANNEL_1);
        verifyInitialFiles(ld, TEST_CHANNEL_1);
    }

    private void verifyInitialFiles(File logDir, String channel) throws Exception {
        logger.trace("Verifying initial files in logdir {}", logDir);
        verifyInfoFile(channel);

        File logFile = new File(logDir, channel + "-0.log");
        assertTrue(logFile.exists());
        assertEquals(options.getPreallocateSize(), logFile.length());

        String[] dirList = logDir.list((dir, name) -> name.endsWith(".log") && name.startsWith(channel));
        assertNotNull(dirList);
        assertEquals(1, dirList.length);
        assertEquals(logFile.getName(), dirList[0]);
    }

    private void verifyInfoFile(String channel) {
        File infoFile = new File(logDir, channel + "-log-info.dat");
        assertTrue(infoFile.exists());
        BsonObject info = readInfoFromFile(infoFile);
        Integer fileNumber = info.getInteger("fileNumber");
        assertNotNull(fileNumber);
        assertEquals(0, (long) fileNumber);
        Integer fileHeadPos = info.getInteger("fileHeadPos");
        assertNotNull(fileHeadPos);
        assertEquals(0, (long) fileHeadPos);
        Integer headPos = info.getInteger("headPos");
        assertNotNull(headPos);
        assertEquals(0, (long) headPos);
        Boolean shutdown = info.getBoolean("shutdown");
        assertNotNull(shutdown);
        assertTrue(shutdown);
    }

    @Test
    public void test_when_restarting_existing_files_are_unchanged(TestContext testContext) throws Exception {
        startLog();
        // Create the files
        test_when_starting_with_existing_log_dir_new_files_are_created(testContext);
        log.close().get();
        log.start().get();
        test_when_starting_with_existing_log_dir_new_files_are_created(testContext);
    }

    @Test
    public void test_when_starting_log_other_channel_files_are_unchanged(TestContext testContext) throws Exception {
        startLog();
        Log log2 = flm.getLog(TEST_CHANNEL_2);
        log2.start().get();
        verifyInitialFiles(logDir, TEST_CHANNEL_2);
    }

    @Test
    public void test_start_with_zeroed_info_file_but_no_log_file(TestContext testContext) throws Exception {
        startLog();
        verifyInitialFiles(logDir, TEST_CHANNEL_1);
        log.close().get();

        File logFile = new File(logDir, TEST_CHANNEL_1 + "-0.log");
        assertTrue(logFile.exists());

        assertTrue(logFile.delete());
        assertFalse(logFile.exists());

        // Start again should succeed as the info file contains just zeros
        startLog();
    }

    @Test
    public void test_start_with_non_zero_file_pos_but_no_log_file(TestContext testContext) throws Exception {
        startLog();
        verifyInitialFiles(logDir, TEST_CHANNEL_1);
        log.close().get();

        File logFile = new File(logDir, TEST_CHANNEL_1 + "-0.log");
        assertTrue(logFile.exists());

        assertTrue(logFile.delete());
        assertFalse(logFile.exists());

        // Now change info file to non zero fileHeadPos
        saveInfo(0, 23, 23, false);

        // Start should now fail
        try {
            startLog();
            fail("Should throw exception");
        } catch (MewException e) {
            // OK
        }
        log = null;
    }

    @Test
    public void test_start_with_non_zero_file_number_but_no_log_file(TestContext testContext) throws Exception {
        startLog();
        verifyInitialFiles(logDir, TEST_CHANNEL_1);
        log.close().get();

        File logFile = new File(logDir, TEST_CHANNEL_1 + "-0.log");
        assertTrue(logFile.exists());

        assertTrue(logFile.delete());
        assertFalse(logFile.exists());

        // Now change info file to non zero fileHeadPos
        saveInfo(1, 0, 0, false);

        // Start should now fail
        try {
            startLog();
            fail("Should throw exception");
        } catch (MewException e) {
            // OK
        }
        log = null;
    }

    @Test
    public void test_start_with_negative_file_number(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            saveInfo(-1, 0, 0, false);
        });
    }

    @Test
    public void test_start_with_negative_file_head_pos(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            saveInfo(0, -1, 0, false);
        });
    }

    @Test
    public void test_start_with_negative_head_pos(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            saveInfo(0, 0, -1, false);
        });
    }


    @Test
    public void test_start_with_missing_file_number(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            BsonObject info = new BsonObject();
            info.put("headPos", 0);
            info.put("fileHeadPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_missing_file_head_pos(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_missing_head_pos(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("fileHeadPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_missing_shutdown(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("fileHeadPos", 0);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_file_number(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", "XYZ");
            info.put("headPos", 0);
            info.put("fileHeadPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_file_head_pos(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("fileHeadPos", "XYZ");
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_head_pos(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", "XYZ");
            info.put("fileHeadPos", 0);
            info.put("shutdown", false);
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_with_invalid_shutdown(TestContext testContext) throws Exception {
        test_start_with_invalid_info_file(testContext, () -> {
            BsonObject info = new BsonObject();
            info.put("fileNumber", 0);
            info.put("headPos", 0);
            info.put("fileHeadPos", 0);
            info.put("shutdown", "XYZ");
            saveFileInfo(info);
        });
    }

    @Test
    public void test_start_max_record_size_too_large(TestContext testContext) throws Exception {
        options = new FileLogManagerOptions().setMaxRecordSize(DEFAULT_MAX_LOG_CHUNK_SIZE + 1);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_start_preallocate_size_too_large(TestContext testContext) throws Exception {
        options = new FileLogManagerOptions().setPreallocateSize(DEFAULT_MAX_LOG_CHUNK_SIZE + 1);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_start_max_record_size_too_small(TestContext testContext) throws Exception {
        options = new FileLogManagerOptions().setMaxRecordSize(0);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_max_log_chunk_size_too_small(TestContext testContext) throws Exception {
        options = new FileLogManagerOptions().setMaxLogChunkSize(0);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void test_start_negative_preallocate_size(TestContext testContext) throws Exception {
        options = new FileLogManagerOptions().setPreallocateSize(-1);
        try {
            startLog();
            fail("should throw exception");
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    private void test_start_with_invalid_info_file(TestContext testContext, Runnable runner) throws Exception {
        startLog();
        verifyInitialFiles(logDir, TEST_CHANNEL_1);
        log.close().get();

        // Now change info file
        runner.run();

        // Start should now fail
        try {
            startLog();
            fail("Should throw exception");
        } catch (MewException e) {
            // OK
        }
        log = null;
    }


}
