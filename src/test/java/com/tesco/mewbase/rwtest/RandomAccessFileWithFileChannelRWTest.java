package com.tesco.mewbase.rwtest;

import com.tesco.mewbase.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by tim on 11/10/16.
 */
public class RandomAccessFileWithFileChannelRWTest implements RWTest {

    private final static Logger log = LoggerFactory.getLogger(RandomAccessFileWithFileChannelRWTest.class);

    private static final int PAGE_SIZE = 4 * 1024;

    @Override
    public int testRead(File testFile) throws Exception {
        // TODO - use FileChannel!!
        RandomAccessFile raf = new RandomAccessFile(testFile, "rw");
        long len = testFile.length();
        int its = (int) (len / PAGE_SIZE);
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
        int its = (int) (len / PAGE_SIZE);
        log.trace("File length is {} iterations are {}", len, its);
        byte[] bytes = TestUtils.randomByteArray(PAGE_SIZE);
        int cnt = 0;
        FileChannel ch = raf.getChannel();
        long pos = 0;
        for (int i = 0; i < its; i++) {
            for (int j = 0; j < bytes.length; j++) {
                cnt += bytes[j];
            }
            ByteBuffer buff = ByteBuffer.wrap(bytes);

            buff.limit(PAGE_SIZE);
            buff.position(0);
            ch.position(pos);
            ch.write(buff);
            pos += PAGE_SIZE;
        }
        raf.close();
        return cnt;
    }
}
