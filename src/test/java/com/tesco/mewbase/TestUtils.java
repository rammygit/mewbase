/*
 *
 *  * Copyright 2014 Red Hat, Inc.
 *  *
 *  * All rights reserved. This program and the accompanying materials
 *  * are made available under the terms of the Eclipse Public License v1.0
 *  * and Apache License v2.0 which accompanies this distribution.
 *  *
 *  *     The Eclipse Public License is available at
 *  *     http://www.eclipse.org/legal/epl-v10.html
 *  *
 *  *     The Apache License v2.0 is available at
 *  *     http://www.opensource.org/licenses/apache2.0.php
 *  *
 *  * You may elect to redistribute this code under either of these licenses.
 *  *
 *
 */

package com.tesco.mewbase;

import java.util.Random;

import static org.junit.Assert.fail;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class TestUtils {

    private static Random random = new Random();

    /**
     * Create an array of random bytes
     *
     * @param length The length of the created array
     * @return the byte array
     */
    public static byte[] randomByteArray(int length) {
        return randomByteArray(length, false, (byte) 0);
    }

    /**
     * Create an array of random bytes
     *
     * @param length    The length of the created array
     * @param avoid     If true, the resulting array will not contain avoidByte
     * @param avoidByte A byte that is not to be included in the resulting array
     * @return an array of random bytes
     */
    public static byte[] randomByteArray(int length, boolean avoid, byte avoidByte) {
        byte[] line = new byte[length];
        for (int i = 0; i < length; i++) {
            byte rand;
            do {
                rand = randomByte();
            } while (avoid && rand == avoidByte);

            line[i] = rand;
        }
        return line;
    }

    /**
     * @return a random byte
     */
    public static byte randomByte() {
        return (byte) ((int) (Math.random() * 255) - 128);
    }

    /**
     * Determine if two byte arrays are equal
     *
     * @param b1 The first byte array to compare
     * @param b2 The second byte array to compare
     * @return true if the byte arrays are equal
     */
    public static boolean byteArraysEqual(byte[] b1, byte[] b2) {
        if (b1.length != b2.length) return false;
        for (int i = 0; i < b1.length; i++) {
            if (b1[i] != b2[i]) return false;
        }
        return true;
    }

}
