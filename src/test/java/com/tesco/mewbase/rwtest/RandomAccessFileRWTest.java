package com.tesco.mewbase.rwtest;

import com.tesco.mewbase.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * Created by tim on 11/10/16.
 */
public class RandomAccessFileRWTest implements RWTest {

    private final static Logger log = LoggerFactory.getLogger(RandomAccessFileRWTest.class);

    private static final int PAGE_SIZE = 4 * 1024;

    @Override
    public int testRead(File testFile) throws Exception {
        RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
        long len = testFile.length();
        int its = (int)(len / PAGE_SIZE);
        log.trace("File length is {} iterations are {}", len, its);
        byte[] bytes = new byte[PAGE_SIZE];
        int bytesRead;
        int cnt = 0;
        while (-1 != (bytesRead = raf.read(bytes))) {
            for (int i = 0; i < bytesRead; i++) {
                cnt += bytes[i];
            }
        }
        raf.close();
        return cnt;
    }

    @Override
    public int testWrite(File testFile) throws Exception {
        RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
        long len = testFile.length();
        int its = (int)(len / PAGE_SIZE);
        log.trace("File length is {} iterations are {}", len, its);
        byte[] bytes = TestUtils.randomByteArray(PAGE_SIZE);
        int cnt = 0;
        for (int i = 0; i < its; i++) {
            for (int j = 0; j < bytes.length; j++) {
                cnt += bytes[j];
            }
            raf.write(bytes, 0, PAGE_SIZE);
        }
        raf.close();
        return cnt;
    }
}
