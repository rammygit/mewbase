package com.tesco.mewbase.rwtest;

import java.io.File;

/**
 * Created by tim on 11/10/16.
 */
public interface RWTest {

    int testRead(File testFile) throws Exception;

    int testWrite(File testFile) throws Exception;

}
