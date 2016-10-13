package com.tesco.mewbase.rwtest;

import com.tesco.mewbase.client.MewException;
import com.tesco.mewbase.log.impl.file.FileLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Created by tim on 11/10/16.
 */
public class RWTestRunner {

    private final static Logger log = LoggerFactory.getLogger(RWTestRunner.class);

    public static void main(String[] args) {
        try {
            new RWTestRunner().start();
        } catch (Throwable t) {
            log.error("Failed to run RWTests", t);
        }
    }

    private static final long FILE_SIZE = 8l * 1024l * 1024l * 1024l;
    private static final int MAX_FILL_BUFFER_SIZE = 10 * 1024 * 1024;
    private static final String TEST_DIR = "rwtest";
    private static final String FILE_NAME = "testfile.dat";

    public void start() {
        File testDir = new File(TEST_DIR);
        if (!testDir.exists()) {
            if (!testDir.mkdirs()) {
                throw new MewException("Failed to create test dir");
            }
        }
        File testFile = new File(testDir, FILE_NAME);
        if (testFile.exists()) {
            if (!testFile.delete()) {
                throw new MewException("Failed to delete test file");
            } else {
                log.trace("Deleted old test file");
            }
        }
        createAndFillFile(testFile, FILE_SIZE);

        while (true) {

            runTest(new RandomAccessFileWithFileChannelRWTest(), testFile);
            runTest(new RandomAccessFileRWTest(), testFile);
        }
    }

    public void runTest(RWTest test, File testFile) {
        {
            long start = System.currentTimeMillis();
            int checkSum = 0;
            try {
                checkSum = test.testWrite(testFile);
            } catch (Exception e) {
                log.error("Failed to run test", e);
            }
            long end = System.currentTimeMillis();
            long fsMB = FILE_SIZE / (1024 * 1024);
            double rate = 1000 * (fsMB / (double) (end - start));
            log.info("{} Write time taken {} ms, rate = {} Mb/s cs {}", test.getClass().getName(), end - start, rate, checkSum);
        }
        {
            long start = System.currentTimeMillis();
            int checkSum = 0;
            try {
                checkSum = test.testRead(testFile);
            } catch (Exception e) {
                log.error("Failed to run test", e);
            }
            long end = System.currentTimeMillis();
            long fsMB = FILE_SIZE / (1024 * 1024);
            double rate = 1000 * (fsMB / (double) (end - start));
            log.info("{} read time taken {} ms, rate = {} Mb/s cs {}", test.getClass().getName(),end - start, rate, checkSum);
        }
    }


    private void createAndFillFile(File file, long size) {
        log.trace("Creating log file {} with size {}", file, size);
        ByteBuffer buff = ByteBuffer.allocate(MAX_FILL_BUFFER_SIZE);
        try (RandomAccessFile rf = new RandomAccessFile(file, "rw")) {
            FileChannel ch = rf.getChannel();
            long pos = 0;
            // We fill the file in chunks in case it is v. big - we don't want to allocate a huge byte buffer
            while (pos < size) {
                int writeSize = (int)Math.min(MAX_FILL_BUFFER_SIZE, size - pos);
                buff.limit(writeSize);
                buff.position(0);
                ch.position(pos);
                ch.write(buff);
                pos += writeSize;
            }
            ch.force(true);
            ch.position(0);
            ch.close();
        } catch (Exception e) {
            throw new MewException("Failed to create test file", e);
        }
        log.trace("Created test file {} with size {}", file, size);
    }

    public static byte[] filledByteArray(int size, byte b) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, 0, size - 1, b);
        return bytes;
    }

}

