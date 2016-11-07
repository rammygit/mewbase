/*
 * Copyright (c) 2011-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Derived from original file JsonObjectTest.java from Vert.x
 */

package com.tesco.mewbase.bson;

import com.tesco.mewbase.TestUtils;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static java.time.format.DateTimeFormatter.ISO_INSTANT;
import static org.junit.Assert.*;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class BsonObjectTest {

    protected BsonObject bsonObject;

    @Before
    public void setUp() throws Exception {
        bsonObject = new BsonObject();
    }

    @Test
    public void testGetInteger() {
        bsonObject.put("foo", 123);
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo"));
        bsonObject.put("bar", "hello");
        try {
            bsonObject.getInteger("bar");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }
        // Put as different Number types
        bsonObject.put("foo", 123L);
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo"));
        bsonObject.put("foo", 123d);
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo"));
        bsonObject.put("foo", 123f);
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo"));
        bsonObject.put("foo", Long.MAX_VALUE);
        assertEquals(Integer.valueOf(-1), bsonObject.getInteger("foo"));

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getInteger("foo"));
        assertNull(bsonObject.getInteger("absent"));

        try {
            bsonObject.getInteger(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetIntegerDefault() {
        bsonObject.put("foo", 123);
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", 321));
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", null));
        bsonObject.put("bar", "hello");
        try {
            bsonObject.getInteger("bar", 123);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }
        // Put as different Number types
        bsonObject.put("foo", 123l);
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", 321));
        bsonObject.put("foo", 123d);
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", 321));
        bsonObject.put("foo", 123f);
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo", 321));
        bsonObject.put("foo", Long.MAX_VALUE);
        assertEquals(Integer.valueOf(-1), bsonObject.getInteger("foo", 321));

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getInteger("foo", 321));
        assertEquals(Integer.valueOf(321), bsonObject.getInteger("absent", 321));
        assertNull(bsonObject.getInteger("foo", null));
        assertNull(bsonObject.getInteger("absent", null));

        try {
            bsonObject.getInteger(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetLong() {
        bsonObject.put("foo", 123l);
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo"));
        bsonObject.put("bar", "hello");
        try {
            bsonObject.getLong("bar");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }
        // Put as different Number types
        bsonObject.put("foo", 123);
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo"));
        bsonObject.put("foo", 123d);
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo"));
        bsonObject.put("foo", 123f);
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo"));
        bsonObject.put("foo", Long.MAX_VALUE);
        assertEquals(Long.valueOf(Long.MAX_VALUE), bsonObject.getLong("foo"));

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getLong("foo"));
        assertNull(bsonObject.getLong("absent"));

        try {
            bsonObject.getLong(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetLongDefault() {
        bsonObject.put("foo", 123l);
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo", 321l));
        assertEquals(Long.valueOf(123), bsonObject.getLong("foo", null));
        bsonObject.put("bar", "hello");
        try {
            bsonObject.getLong("bar", 123l);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }
        // Put as different Number types
        bsonObject.put("foo", 123);
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo", 321l));
        bsonObject.put("foo", 123d);
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo", 321l));
        bsonObject.put("foo", 123f);
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo", 321l));
        bsonObject.put("foo", Long.MAX_VALUE);
        assertEquals(Long.valueOf(Long.MAX_VALUE), bsonObject.getLong("foo", 321l));

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getLong("foo", 321l));
        assertEquals(Long.valueOf(321l), bsonObject.getLong("absent", 321l));
        assertNull(bsonObject.getLong("foo", null));
        assertNull(bsonObject.getLong("absent", null));

        try {
            bsonObject.getLong(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetFloat() {
        bsonObject.put("foo", 123f);
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo"));
        bsonObject.put("bar", "hello");
        try {
            bsonObject.getFloat("bar");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }
        // Put as different Number types
        bsonObject.put("foo", 123);
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo"));
        bsonObject.put("foo", 123d);
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo"));
        bsonObject.put("foo", 123f);
        assertEquals(Float.valueOf(123l), bsonObject.getFloat("foo"));

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getFloat("foo"));
        assertNull(bsonObject.getFloat("absent"));

        try {
            bsonObject.getFloat(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetFloatDefault() {
        bsonObject.put("foo", 123f);
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo", 321f));
        assertEquals(Float.valueOf(123), bsonObject.getFloat("foo", null));
        bsonObject.put("bar", "hello");
        try {
            bsonObject.getFloat("bar", 123f);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }
        // Put as different Number types
        bsonObject.put("foo", 123);
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo", 321f));
        bsonObject.put("foo", 123d);
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo", 321f));
        bsonObject.put("foo", 123l);
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo", 321f));

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getFloat("foo", 321f));
        assertEquals(Float.valueOf(321f), bsonObject.getFloat("absent", 321f));
        assertNull(bsonObject.getFloat("foo", null));
        assertNull(bsonObject.getFloat("absent", null));

        try {
            bsonObject.getFloat(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetDouble() {
        bsonObject.put("foo", 123d);
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo"));
        bsonObject.put("bar", "hello");
        try {
            bsonObject.getDouble("bar");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }
        // Put as different Number types
        bsonObject.put("foo", 123);
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo"));
        bsonObject.put("foo", 123l);
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo"));
        bsonObject.put("foo", 123f);
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo"));

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getDouble("foo"));
        assertNull(bsonObject.getDouble("absent"));

        try {
            bsonObject.getDouble(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetDoubleDefault() {
        bsonObject.put("foo", 123d);
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo", 321d));
        assertEquals(Double.valueOf(123), bsonObject.getDouble("foo", null));
        bsonObject.put("bar", "hello");
        try {
            bsonObject.getDouble("bar", 123d);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }
        // Put as different Number types
        bsonObject.put("foo", 123);
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo", 321d));
        bsonObject.put("foo", 123f);
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo", 321d));
        bsonObject.put("foo", 123l);
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo", 321d));

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getDouble("foo", 321d));
        assertEquals(Double.valueOf(321d), bsonObject.getDouble("absent", 321d));
        assertNull(bsonObject.getDouble("foo", null));
        assertNull(bsonObject.getDouble("absent", null));

        try {
            bsonObject.getDouble(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetString() {
        bsonObject.put("foo", "bar");
        assertEquals("bar", bsonObject.getString("foo"));
        bsonObject.put("bar", 123);
        try {
            bsonObject.getString("bar");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getString("foo"));
        assertNull(bsonObject.getString("absent"));

        try {
            bsonObject.getString(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetStringDefault() {
        bsonObject.put("foo", "bar");
        assertEquals("bar", bsonObject.getString("foo", "wibble"));
        assertEquals("bar", bsonObject.getString("foo", null));
        bsonObject.put("bar", 123);
        try {
            bsonObject.getString("bar", "wibble");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getString("foo", "wibble"));
        assertEquals("wibble", bsonObject.getString("absent", "wibble"));
        assertNull(bsonObject.getString("foo", null));
        assertNull(bsonObject.getString("absent", null));

        try {
            bsonObject.getString(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetBoolean() {
        bsonObject.put("foo", true);
        assertEquals(true, bsonObject.getBoolean("foo"));
        bsonObject.put("foo", false);
        assertEquals(false, bsonObject.getBoolean("foo"));
        bsonObject.put("bar", 123);
        try {
            bsonObject.getBoolean("bar");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getBoolean("foo"));
        assertNull(bsonObject.getBoolean("absent"));

        try {
            bsonObject.getBoolean(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }

    @Test
    public void testGetBooleanDefault() {
        bsonObject.put("foo", true);
        assertEquals(true, bsonObject.getBoolean("foo", false));
        assertEquals(true, bsonObject.getBoolean("foo", null));
        bsonObject.put("foo", false);
        assertEquals(false, bsonObject.getBoolean("foo", true));
        assertEquals(false, bsonObject.getBoolean("foo", null));
        bsonObject.put("bar", 123);
        try {
            bsonObject.getBoolean("bar", true);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        // Null and absent values
        bsonObject.putNull("foo");
        assertNull(bsonObject.getBoolean("foo", true));
        assertNull(bsonObject.getBoolean("foo", false));
        assertEquals(true, bsonObject.getBoolean("absent", true));
        assertEquals(false, bsonObject.getBoolean("absent", false));

        try {
            bsonObject.getBoolean(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }

    }


    @Test
    public void testGetBinary() {
        byte[] bytes = TestUtils.randomByteArray(100);
        bsonObject.put("foo", bytes);
        assertTrue(TestUtils.byteArraysEqual(bytes, bsonObject.getBinary("foo")));

        // Can also get as string:
        String val = bsonObject.getString("foo");
        assertNotNull(val);
        byte[] retrieved = Base64.getDecoder().decode(val);
        assertTrue(TestUtils.byteArraysEqual(bytes, retrieved));

        bsonObject.put("foo", 123);
        try {
            bsonObject.getBinary("foo");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        bsonObject.putNull("foo");
        assertNull(bsonObject.getBinary("foo"));
        assertNull(bsonObject.getBinary("absent"));
        try {
            bsonObject.getBinary(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
        try {
            bsonObject.getBinary(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testGetInstant() {
        Instant now = Instant.now();
        bsonObject.put("foo", now);
        assertEquals(now, bsonObject.getInstant("foo"));

        // Can also get as string:
        String val = bsonObject.getString("foo");
        assertNotNull(val);
        Instant retrieved = Instant.from(ISO_INSTANT.parse(val));
        assertEquals(now, retrieved);

        bsonObject.put("foo", 123);
        try {
            bsonObject.getInstant("foo");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        bsonObject.putNull("foo");
        assertNull(bsonObject.getInstant("foo"));
        assertNull(bsonObject.getInstant("absent"));
        try {
            bsonObject.getInstant(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
        try {
            bsonObject.getInstant(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testGetBinaryDefault() {
        byte[] bytes = TestUtils.randomByteArray(100);
        byte[] defBytes = TestUtils.randomByteArray(100);
        bsonObject.put("foo", bytes);
        assertTrue(TestUtils.byteArraysEqual(bytes, bsonObject.getBinary("foo", defBytes)));
        assertTrue(TestUtils.byteArraysEqual(bytes, bsonObject.getBinary("foo", null)));

        bsonObject.put("foo", 123);
        try {
            bsonObject.getBinary("foo", defBytes);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        bsonObject.putNull("foo");
        assertNull(bsonObject.getBinary("foo", defBytes));
        assertTrue(TestUtils.byteArraysEqual(defBytes, bsonObject.getBinary("absent", defBytes)));
        assertNull(bsonObject.getBinary("foo", null));
        assertNull(bsonObject.getBinary("absent", null));
        try {
            bsonObject.getBinary(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testGetInstantDefault() {
        Instant now = Instant.now();
        Instant later = now.plus(1, ChronoUnit.DAYS);
        bsonObject.put("foo", now);
        assertEquals(now, bsonObject.getInstant("foo", later));
        assertEquals(now, bsonObject.getInstant("foo", null));

        bsonObject.put("foo", 123);
        try {
            bsonObject.getInstant("foo", later);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        bsonObject.putNull("foo");
        assertNull(bsonObject.getInstant("foo", later));
        assertEquals(later, bsonObject.getInstant("absent", later));
        assertNull(bsonObject.getInstant("foo", null));
        assertNull(bsonObject.getInstant("absent", null));
        try {
            bsonObject.getInstant(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testGetBsonObject() {
        BsonObject obj = new BsonObject().put("blah", "wibble");
        bsonObject.put("foo", obj);
        assertEquals(obj, bsonObject.getBsonObject("foo"));

        bsonObject.put("foo", "hello");
        try {
            bsonObject.getBsonObject("foo");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        bsonObject.putNull("foo");
        assertNull(bsonObject.getBsonObject("foo"));
        assertNull(bsonObject.getBsonObject("absent"));
        try {
            bsonObject.getBsonObject(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testGetBsonObjectDefault() {
        BsonObject obj = new BsonObject().put("blah", "wibble");
        BsonObject def = new BsonObject().put("eek", "quuz");
        bsonObject.put("foo", obj);
        assertEquals(obj, bsonObject.getBsonObject("foo", def));
        assertEquals(obj, bsonObject.getBsonObject("foo", null));

        bsonObject.put("foo", "hello");
        try {
            bsonObject.getBsonObject("foo", def);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        bsonObject.putNull("foo");
        assertNull(bsonObject.getBsonObject("foo", def));
        assertEquals(def, bsonObject.getBsonObject("absent", def));
        assertNull(bsonObject.getBsonObject("foo", null));
        assertNull(bsonObject.getBsonObject("absent", null));
        try {
            bsonObject.getBsonObject(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testGetBsonArray() {
        BsonArray arr = new BsonArray().add("blah").add("wibble");
        bsonObject.put("foo", arr);
        assertEquals(arr, bsonObject.getBsonArray("foo"));

        bsonObject.put("foo", "hello");
        try {
            bsonObject.getBsonArray("foo");
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        bsonObject.putNull("foo");
        assertNull(bsonObject.getBsonArray("foo"));
        assertNull(bsonObject.getBsonArray("absent"));
        try {
            bsonObject.getBsonArray(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testGetBsonArrayDefault() {
        BsonArray arr = new BsonArray().add("blah").add("wibble");
        BsonArray def = new BsonArray().add("quux").add("eek");
        bsonObject.put("foo", arr);
        assertEquals(arr, bsonObject.getBsonArray("foo", def));
        assertEquals(arr, bsonObject.getBsonArray("foo", null));

        bsonObject.put("foo", "hello");
        try {
            bsonObject.getBsonArray("foo", def);
            fail();
        } catch (ClassCastException e) {
            // Ok
        }

        bsonObject.putNull("foo");
        assertNull(bsonObject.getBsonArray("foo", def));
        assertEquals(def, bsonObject.getBsonArray("absent", def));
        assertNull(bsonObject.getBsonArray("foo", null));
        assertNull(bsonObject.getBsonArray("absent", null));
        try {
            bsonObject.getBsonArray(null, null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testGetValue() {
        bsonObject.put("foo", 123);
        assertEquals(123, bsonObject.getValue("foo"));
        bsonObject.put("foo", 123l);
        assertEquals(123l, bsonObject.getValue("foo"));
        bsonObject.put("foo", 123f);
        assertEquals(123f, bsonObject.getValue("foo"));
        bsonObject.put("foo", 123d);
        assertEquals(123d, bsonObject.getValue("foo"));
        bsonObject.put("foo", false);
        assertEquals(false, bsonObject.getValue("foo"));
        bsonObject.put("foo", true);
        assertEquals(true, bsonObject.getValue("foo"));
        bsonObject.put("foo", "bar");
        assertEquals("bar", bsonObject.getValue("foo"));
        BsonObject obj = new BsonObject().put("blah", "wibble");
        bsonObject.put("foo", obj);
        assertEquals(obj, bsonObject.getValue("foo"));
        BsonArray arr = new BsonArray().add("blah").add("wibble");
        bsonObject.put("foo", arr);
        assertEquals(arr, bsonObject.getValue("foo"));
        byte[] bytes = TestUtils.randomByteArray(100);
        bsonObject.put("foo", bytes);
        assertTrue(TestUtils.byteArraysEqual(bytes, Base64.getDecoder().decode((String) bsonObject.getValue("foo"))));
        bsonObject.putNull("foo");
        assertNull(bsonObject.getValue("foo"));
        assertNull(bsonObject.getValue("absent"));
        // BsonObject with inner Map
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> innerMap = new HashMap<>();
        innerMap.put("blah", "wibble");
        map.put("foo", innerMap);
        bsonObject = new BsonObject(map);
        obj = (BsonObject) bsonObject.getValue("foo");
        assertEquals("wibble", obj.getString("blah"));
        // BsonObject with inner List
        map = new HashMap<>();
        List<Object> innerList = new ArrayList<>();
        innerList.add("blah");
        map.put("foo", innerList);
        bsonObject = new BsonObject(map);
        arr = (BsonArray) bsonObject.getValue("foo");
        assertEquals("blah", arr.getString(0));
    }

    @Test
    public void testGetValueDefault() {
        bsonObject.put("foo", 123);
        assertEquals(123, bsonObject.getValue("foo", "blah"));
        assertEquals(123, bsonObject.getValue("foo", null));
        bsonObject.put("foo", 123l);
        assertEquals(123l, bsonObject.getValue("foo", "blah"));
        assertEquals(123l, bsonObject.getValue("foo", null));
        bsonObject.put("foo", 123f);
        assertEquals(123f, bsonObject.getValue("foo", "blah"));
        assertEquals(123f, bsonObject.getValue("foo", null));
        bsonObject.put("foo", 123d);
        assertEquals(123d, bsonObject.getValue("foo", "blah"));
        assertEquals(123d, bsonObject.getValue("foo", null));
        bsonObject.put("foo", false);
        assertEquals(false, bsonObject.getValue("foo", "blah"));
        assertEquals(false, bsonObject.getValue("foo", null));
        bsonObject.put("foo", true);
        assertEquals(true, bsonObject.getValue("foo", "blah"));
        assertEquals(true, bsonObject.getValue("foo", null));
        bsonObject.put("foo", "bar");
        assertEquals("bar", bsonObject.getValue("foo", "blah"));
        assertEquals("bar", bsonObject.getValue("foo", null));
        BsonObject obj = new BsonObject().put("blah", "wibble");
        bsonObject.put("foo", obj);
        assertEquals(obj, bsonObject.getValue("foo", "blah"));
        assertEquals(obj, bsonObject.getValue("foo", null));
        BsonArray arr = new BsonArray().add("blah").add("wibble");
        bsonObject.put("foo", arr);
        assertEquals(arr, bsonObject.getValue("foo", "blah"));
        assertEquals(arr, bsonObject.getValue("foo", null));
        byte[] bytes = TestUtils.randomByteArray(100);
        bsonObject.put("foo", bytes);
        assertTrue(TestUtils.byteArraysEqual(bytes, Base64.getDecoder().decode((String) bsonObject.getValue("foo", "blah"))));
        assertTrue(TestUtils.byteArraysEqual(bytes, Base64.getDecoder().decode((String) bsonObject.getValue("foo", null))));
        bsonObject.putNull("foo");
        assertNull(bsonObject.getValue("foo", "blah"));
        assertNull(bsonObject.getValue("foo", null));
        assertEquals("blah", bsonObject.getValue("absent", "blah"));
        assertNull(bsonObject.getValue("absent", null));
    }

    @Test
    public void testContainsKey() {
        bsonObject.put("foo", "bar");
        assertTrue(bsonObject.containsKey("foo"));
        bsonObject.putNull("foo");
        assertTrue(bsonObject.containsKey("foo"));
        assertFalse(bsonObject.containsKey("absent"));
    }

    @Test
    public void testFieldNames() {
        bsonObject.put("foo", "bar");
        bsonObject.put("eek", 123);
        bsonObject.put("flib", new BsonObject());
        Set<String> fieldNames = bsonObject.fieldNames();
        assertEquals(3, fieldNames.size());
        assertTrue(fieldNames.contains("foo"));
        assertTrue(fieldNames.contains("eek"));
        assertTrue(fieldNames.contains("flib"));
        bsonObject.remove("foo");
        assertEquals(2, fieldNames.size());
        assertFalse(fieldNames.contains("foo"));
    }

    @Test
    public void testSize() {
        assertEquals(0, bsonObject.size());
        bsonObject.put("foo", "bar");
        assertEquals(1, bsonObject.size());
        bsonObject.put("bar", 123);
        assertEquals(2, bsonObject.size());
        bsonObject.putNull("wibble");
        assertEquals(3, bsonObject.size());
        bsonObject.remove("wibble");
        assertEquals(2, bsonObject.size());
        bsonObject.clear();
        assertEquals(0, bsonObject.size());
    }

    enum SomeEnum {
        FOO, BAR
    }

    @Test
    public void testPutEnum() {
        assertSame(bsonObject, bsonObject.put("foo", SomeEnum.FOO));
        assertEquals(SomeEnum.FOO.toString(), bsonObject.getString("foo"));
        assertTrue(bsonObject.containsKey("foo"));
        try {
            bsonObject.put(null, SomeEnum.FOO);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutString() {
        assertSame(bsonObject, bsonObject.put("foo", "bar"));
        assertEquals("bar", bsonObject.getString("foo"));
        bsonObject.put("quux", "wibble");
        assertEquals("wibble", bsonObject.getString("quux"));
        assertEquals("bar", bsonObject.getString("foo"));
        bsonObject.put("foo", "blah");
        assertEquals("blah", bsonObject.getString("foo"));
        bsonObject.put("foo", (String) null);
        assertTrue(bsonObject.containsKey("foo"));
        try {
            bsonObject.put(null, "blah");
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutCharSequence() {
        assertSame(bsonObject, bsonObject.put("foo", new StringBuilder("bar")));
        assertEquals("bar", bsonObject.getString("foo"));
        assertEquals("bar", bsonObject.getString("foo", "def"));
        bsonObject.put("quux", new StringBuilder("wibble"));
        assertEquals("wibble", bsonObject.getString("quux"));
        assertEquals("bar", bsonObject.getString("foo"));
        bsonObject.put("foo", new StringBuilder("blah"));
        assertEquals("blah", bsonObject.getString("foo"));
        bsonObject.put("foo", (CharSequence) null);
        assertTrue(bsonObject.containsKey("foo"));
        try {
            bsonObject.put(null, (CharSequence) "blah");
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutInteger() {
        assertSame(bsonObject, bsonObject.put("foo", 123));
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo"));
        bsonObject.put("quux", 321);
        assertEquals(Integer.valueOf(321), bsonObject.getInteger("quux"));
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("foo"));
        bsonObject.put("foo", 456);
        assertEquals(Integer.valueOf(456), bsonObject.getInteger("foo"));
        bsonObject.put("foo", (Integer) null);
        assertTrue(bsonObject.containsKey("foo"));
        try {
            bsonObject.put(null, 123);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutLong() {
        assertSame(bsonObject, bsonObject.put("foo", 123l));
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo"));
        bsonObject.put("quux", 321l);
        assertEquals(Long.valueOf(321l), bsonObject.getLong("quux"));
        assertEquals(Long.valueOf(123l), bsonObject.getLong("foo"));
        bsonObject.put("foo", 456l);
        assertEquals(Long.valueOf(456l), bsonObject.getLong("foo"));
        bsonObject.put("foo", (Long) null);
        assertTrue(bsonObject.containsKey("foo"));

        try {
            bsonObject.put(null, 123l);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutFloat() {
        assertSame(bsonObject, bsonObject.put("foo", 123f));
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo"));
        bsonObject.put("quux", 321f);
        assertEquals(Float.valueOf(321f), bsonObject.getFloat("quux"));
        assertEquals(Float.valueOf(123f), bsonObject.getFloat("foo"));
        bsonObject.put("foo", 456f);
        assertEquals(Float.valueOf(456f), bsonObject.getFloat("foo"));
        bsonObject.put("foo", (Float) null);
        assertTrue(bsonObject.containsKey("foo"));

        try {
            bsonObject.put(null, 1.2f);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutDouble() {
        assertSame(bsonObject, bsonObject.put("foo", 123d));
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo"));
        bsonObject.put("quux", 321d);
        assertEquals(Double.valueOf(321d), bsonObject.getDouble("quux"));
        assertEquals(Double.valueOf(123d), bsonObject.getDouble("foo"));
        bsonObject.put("foo", 456d);
        assertEquals(Double.valueOf(456d), bsonObject.getDouble("foo"));
        bsonObject.put("foo", (Double) null);
        assertTrue(bsonObject.containsKey("foo"));
        try {
            bsonObject.put(null, 1.23d);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutBoolean() {
        assertSame(bsonObject, bsonObject.put("foo", true));
        assertEquals(true, bsonObject.getBoolean("foo"));
        bsonObject.put("quux", true);
        assertEquals(true, bsonObject.getBoolean("quux"));
        assertEquals(true, bsonObject.getBoolean("foo"));
        bsonObject.put("foo", true);
        assertEquals(true, bsonObject.getBoolean("foo"));
        bsonObject.put("foo", (Boolean) null);
        assertTrue(bsonObject.containsKey("foo"));
        try {
            bsonObject.put(null, false);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutBsonObject() {
        BsonObject obj1 = new BsonObject().put("blah", "wibble");
        BsonObject obj2 = new BsonObject().put("eeek", "flibb");
        BsonObject obj3 = new BsonObject().put("floob", "plarp");
        assertSame(bsonObject, bsonObject.put("foo", obj1));
        assertEquals(obj1, bsonObject.getBsonObject("foo"));
        bsonObject.put("quux", obj2);
        assertEquals(obj2, bsonObject.getBsonObject("quux"));
        assertEquals(obj1, bsonObject.getBsonObject("foo"));
        bsonObject.put("foo", obj3);
        assertEquals(obj3, bsonObject.getBsonObject("foo"));
        bsonObject.put("foo", (BsonObject) null);
        assertTrue(bsonObject.containsKey("foo"));
        try {
            bsonObject.put(null, new BsonObject());
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutBsonArray() {
        BsonArray obj1 = new BsonArray().add("parp");
        BsonArray obj2 = new BsonArray().add("fleep");
        BsonArray obj3 = new BsonArray().add("woob");

        assertSame(bsonObject, bsonObject.put("foo", obj1));
        assertEquals(obj1, bsonObject.getBsonArray("foo"));
        bsonObject.put("quux", obj2);
        assertEquals(obj2, bsonObject.getBsonArray("quux"));
        assertEquals(obj1, bsonObject.getBsonArray("foo"));
        bsonObject.put("foo", obj3);
        assertEquals(obj3, bsonObject.getBsonArray("foo"));

        bsonObject.put("foo", (BsonArray) null);
        assertTrue(bsonObject.containsKey("foo"));


        try {
            bsonObject.put(null, new BsonArray());
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutBinary() {
        byte[] bin1 = TestUtils.randomByteArray(100);
        byte[] bin2 = TestUtils.randomByteArray(100);
        byte[] bin3 = TestUtils.randomByteArray(100);

        assertSame(bsonObject, bsonObject.put("foo", bin1));
        assertTrue(TestUtils.byteArraysEqual(bin1, bsonObject.getBinary("foo")));
        bsonObject.put("quux", bin2);
        assertTrue(TestUtils.byteArraysEqual(bin2, bsonObject.getBinary("quux")));
        assertTrue(TestUtils.byteArraysEqual(bin1, bsonObject.getBinary("foo")));
        bsonObject.put("foo", bin3);
        assertTrue(TestUtils.byteArraysEqual(bin3, bsonObject.getBinary("foo")));

        bsonObject.put("foo", (byte[]) null);
        assertTrue(bsonObject.containsKey("foo"));

        try {
            bsonObject.put(null, bin1);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutInstant() {
        Instant bin1 = Instant.now();
        Instant bin2 = bin1.plus(1, ChronoUnit.DAYS);
        Instant bin3 = bin1.plus(1, ChronoUnit.MINUTES);

        assertSame(bsonObject, bsonObject.put("foo", bin1));
        assertEquals(bin1, bsonObject.getInstant("foo"));
        bsonObject.put("quux", bin2);
        assertEquals(bin2, bsonObject.getInstant("quux"));
        assertEquals(bin1, bsonObject.getInstant("foo"));
        bsonObject.put("foo", bin3);
        assertEquals(bin3, bsonObject.getInstant("foo"));

        bsonObject.put("foo", (Instant) null);
        assertTrue(bsonObject.containsKey("foo"));

        try {
            bsonObject.put(null, bin1);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutNull() {
        assertSame(bsonObject, bsonObject.putNull("foo"));
        assertTrue(bsonObject.containsKey("foo"));
        assertSame(bsonObject, bsonObject.putNull("bar"));
        assertTrue(bsonObject.containsKey("bar"));
        try {
            bsonObject.putNull(null);
            fail();
        } catch (NullPointerException e) {
            // OK
        }
    }

    @Test
    public void testPutValue() {
        bsonObject.put("str", (Object) "bar");
        bsonObject.put("int", (Object) (Integer.valueOf(123)));
        bsonObject.put("long", (Object) (Long.valueOf(123l)));
        bsonObject.put("float", (Object) (Float.valueOf(1.23f)));
        bsonObject.put("double", (Object) (Double.valueOf(1.23d)));
        bsonObject.put("boolean", (Object) true);
        byte[] bytes = TestUtils.randomByteArray(10);
        bsonObject.put("binary", (Object) (bytes));
        Instant now = Instant.now();
        bsonObject.put("instant", now);
        BsonObject obj = new BsonObject().put("foo", "blah");
        BsonArray arr = new BsonArray().add("quux");
        bsonObject.put("obj", (Object) obj);
        bsonObject.put("arr", (Object) arr);
        assertEquals("bar", bsonObject.getString("str"));
        assertEquals(Integer.valueOf(123), bsonObject.getInteger("int"));
        assertEquals(Long.valueOf(123l), bsonObject.getLong("long"));
        assertEquals(Float.valueOf(1.23f), bsonObject.getFloat("float"));
        assertEquals(Double.valueOf(1.23d), bsonObject.getDouble("double"));
        assertTrue(TestUtils.byteArraysEqual(bytes, bsonObject.getBinary("binary")));
        assertEquals(now, bsonObject.getInstant("instant"));
        assertEquals(obj, bsonObject.getBsonObject("obj"));
        assertEquals(arr, bsonObject.getBsonArray("arr"));
        try {
            bsonObject.put("inv", new SomeClass());
            fail();
        } catch (IllegalStateException e) {
            // OK
        }
        try {
            bsonObject.put("inv", new BigDecimal(123));
            fail();
        } catch (IllegalStateException e) {
            // OK
        }
        try {
            bsonObject.put("inv", new Date());
            fail();
        } catch (IllegalStateException e) {
            // OK
        }

    }

    @Test
    public void testMergeIn1() {
        BsonObject obj1 = new BsonObject().put("foo", "bar");
        BsonObject obj2 = new BsonObject().put("eek", "flurb");
        obj1.mergeIn(obj2);
        assertEquals(2, obj1.size());
        assertEquals("bar", obj1.getString("foo"));
        assertEquals("flurb", obj1.getString("eek"));
        assertEquals(1, obj2.size());
        assertEquals("flurb", obj2.getString("eek"));
    }

    @Test
    public void testMergeIn2() {
        BsonObject obj1 = new BsonObject().put("foo", "bar");
        BsonObject obj2 = new BsonObject().put("foo", "flurb");
        obj1.mergeIn(obj2);
        assertEquals(1, obj1.size());
        assertEquals("flurb", obj1.getString("foo"));
        assertEquals(1, obj2.size());
        assertEquals("flurb", obj2.getString("foo"));
    }

    @Test
    public void testEncode() throws Exception {
        bsonObject.put("mystr", "foo");
        bsonObject.put("mycharsequence", new StringBuilder("oob"));
        bsonObject.put("myint", 123);
        bsonObject.put("mylong", 1234l);
        bsonObject.put("myfloat", 1.23f);
        bsonObject.put("mydouble", 2.34d);
        bsonObject.put("myboolean", true);
        byte[] bytes = TestUtils.randomByteArray(10);
        bsonObject.put("mybinary", bytes);
        Instant now = Instant.now();
        bsonObject.put("myinstant", now);
        bsonObject.putNull("mynull");
        bsonObject.put("myobj", new BsonObject().put("foo", "bar"));
        bsonObject.put("myarr", new BsonArray().add("foo").add(123));
        Buffer encoded = bsonObject.encode();
        BsonObject obj = new BsonObject(encoded);
        assertEquals("foo", obj.getString("mystr"));
        assertEquals("oob", obj.getString("mycharsequence"));
        assertEquals(Integer.valueOf(123), obj.getInteger("myint"));
        assertEquals(Long.valueOf(1234), obj.getLong("mylong"));
        assertEquals(Float.valueOf(1.23f), obj.getFloat("myfloat"));
        assertEquals(Double.valueOf(2.34d), obj.getDouble("mydouble"));
        assertTrue(obj.getBoolean("myboolean"));
        assertTrue(TestUtils.byteArraysEqual(bytes, obj.getBinary("mybinary")));
        assertEquals(now, obj.getInstant("myinstant"));
        assertTrue(obj.containsKey("mynull"));
        BsonObject nestedObj = obj.getBsonObject("myobj");
        assertEquals("bar", nestedObj.getString("foo"));
        BsonArray nestedArr = obj.getBsonArray("myarr");
        assertEquals("foo", nestedArr.getString(0));
        assertEquals(Integer.valueOf(123), Integer.valueOf(nestedArr.getInteger(1)));
    }

    @Test
    public void testEncodeSize() throws Exception {
        bsonObject.put("foo", "bar");
        Buffer encoded = bsonObject.encode();
        int length = encoded.getIntLE(0);
        assertEquals(encoded.length(), length);
    }

    @Test
    public void testInvalidJson() {
        Buffer invalid = Buffer.buffer(TestUtils.randomByteArray(100));
        try {
            new BsonObject(invalid);
            fail();
        } catch (DecodeException e) {
            // OK
        }
    }

    @Test
    public void testClear() {
        bsonObject.put("foo", "bar");
        bsonObject.put("quux", 123);
        assertEquals(2, bsonObject.size());
        bsonObject.clear();
        assertEquals(0, bsonObject.size());
        assertNull(bsonObject.getValue("foo"));
        assertNull(bsonObject.getValue("quux"));
    }

    @Test
    public void testIsEmpty() {
        assertTrue(bsonObject.isEmpty());
        bsonObject.put("foo", "bar");
        bsonObject.put("quux", 123);
        assertFalse(bsonObject.isEmpty());
        bsonObject.clear();
        assertTrue(bsonObject.isEmpty());
    }

    @Test
    public void testRemove() {
        bsonObject.put("mystr", "bar");
        bsonObject.put("myint", 123);
        assertEquals("bar", bsonObject.remove("mystr"));
        assertNull(bsonObject.getValue("mystr"));
        assertEquals(123, bsonObject.remove("myint"));
        assertNull(bsonObject.getValue("myint"));
        assertTrue(bsonObject.isEmpty());
    }

    @Test
    public void testIterator() {
        bsonObject.put("foo", "bar");
        bsonObject.put("quux", 123);
        BsonObject obj = createBsonObject();
        bsonObject.put("wibble", obj);
        Iterator<Map.Entry<String, Object>> iter = bsonObject.iterator();
        assertTrue(iter.hasNext());
        Map.Entry<String, Object> entry = iter.next();
        assertEquals("foo", entry.getKey());
        assertEquals("bar", entry.getValue());
        assertTrue(iter.hasNext());
        entry = iter.next();
        assertEquals("quux", entry.getKey());
        assertEquals(123, entry.getValue());
        assertTrue(iter.hasNext());
        entry = iter.next();
        assertEquals("wibble", entry.getKey());
        assertEquals(obj, entry.getValue());
        assertFalse(iter.hasNext());
        iter.remove();
        assertFalse(obj.containsKey("wibble"));
        assertEquals(2, bsonObject.size());
    }

    @Test
    public void testIteratorDoesntChangeObject() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("nestedMap", new HashMap<>());
        map.put("nestedList", new ArrayList<>());
        BsonObject obj = new BsonObject(map);
        Iterator<Map.Entry<String, Object>> iter = obj.iterator();
        Map.Entry<String, Object> entry1 = iter.next();
        assertEquals("nestedMap", entry1.getKey());
        Object val1 = entry1.getValue();
        assertTrue(val1 instanceof BsonObject);
        Map.Entry<String, Object> entry2 = iter.next();
        assertEquals("nestedList", entry2.getKey());
        Object val2 = entry2.getValue();
        assertTrue(val2 instanceof BsonArray);
        assertTrue(map.get("nestedMap") instanceof HashMap);
        assertTrue(map.get("nestedList") instanceof ArrayList);
    }

    @Test
    public void testStream() {
        bsonObject.put("foo", "bar");
        bsonObject.put("quux", 123);
        BsonObject obj = createBsonObject();
        bsonObject.put("wibble", obj);
        List<Map.Entry<String, Object>> list = bsonObject.stream().collect(Collectors.toList());
        Iterator<Map.Entry<String, Object>> iter = list.iterator();
        assertTrue(iter.hasNext());
        Map.Entry<String, Object> entry = iter.next();
        assertEquals("foo", entry.getKey());
        assertEquals("bar", entry.getValue());
        assertTrue(iter.hasNext());
        entry = iter.next();
        assertEquals("quux", entry.getKey());
        assertEquals(123, entry.getValue());
        assertTrue(iter.hasNext());
        entry = iter.next();
        assertEquals("wibble", entry.getKey());
        assertEquals(obj, entry.getValue());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testCopy() {
        bsonObject.put("foo", "bar");
        bsonObject.put("quux", 123);
        BsonObject obj = createBsonObject();
        bsonObject.put("wibble", obj);
        bsonObject.put("eek", new StringBuilder("blah")); // CharSequence
        BsonObject copy = bsonObject.copy();
        assertNotSame(bsonObject, copy);
        assertEquals(bsonObject, copy);
        copy.put("blah", "flib");
        assertFalse(bsonObject.containsKey("blah"));
        copy.remove("foo");
        assertFalse(copy.containsKey("foo"));
        assertTrue(bsonObject.containsKey("foo"));
        bsonObject.put("oob", "flarb");
        assertFalse(copy.containsKey("oob"));
        bsonObject.remove("quux");
        assertFalse(bsonObject.containsKey("quux"));
        assertTrue(copy.containsKey("quux"));
        BsonObject nested = bsonObject.getBsonObject("wibble");
        BsonObject nestedCopied = copy.getBsonObject("wibble");
        assertNotSame(nested, nestedCopied);
        assertEquals(nested, nestedCopied);
        assertEquals("blah", copy.getString("eek"));
    }

    @Test
    public void testInvalidValsOnCopy1() {
        Map<String, Object> invalid = new HashMap<>();
        invalid.put("foo", new SomeClass());
        BsonObject object = new BsonObject(invalid);
        try {
            object.copy();
            fail();
        } catch (IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void testInvalidValsOnCopy2() {
        Map<String, Object> invalid = new HashMap<>();
        Map<String, Object> invalid2 = new HashMap<>();
        invalid2.put("foo", new SomeClass());
        invalid.put("bar", invalid2);
        BsonObject object = new BsonObject(invalid);
        try {
            object.copy();
            fail();
        } catch (IllegalStateException e) {
            // OK
        }
    }

    @Test
    public void testInvalidValsOnCopy3() {
        Map<String, Object> invalid = new HashMap<>();
        List<Object> invalid2 = new ArrayList<>();
        invalid2.add(new SomeClass());
        invalid.put("bar", invalid2);
        BsonObject object = new BsonObject(invalid);
        try {
            object.copy();
            fail();
        } catch (IllegalStateException e) {
            // OK
        }
    }

    class SomeClass {
    }

    @Test
    public void testGetMap() {
        bsonObject.put("foo", "bar");
        bsonObject.put("quux", 123);
        BsonObject obj = createBsonObject();
        bsonObject.put("wibble", obj);
        Map<String, Object> map = bsonObject.getMap();
        map.remove("foo");
        assertFalse(bsonObject.containsKey("foo"));
        map.put("bleep", "flarp");
        assertTrue(bsonObject.containsKey("bleep"));
        bsonObject.remove("quux");
        assertFalse(map.containsKey("quux"));
        bsonObject.put("wooble", "plink");
        assertTrue(map.containsKey("wooble"));
        assertSame(obj, map.get("wibble"));
    }

    @Test
    public void testCreateFromMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("foo", "bar");
        map.put("quux", 123);
        BsonObject obj = new BsonObject(map);
        assertEquals("bar", obj.getString("foo"));
        assertEquals(Integer.valueOf(123), obj.getInteger("quux"));
        assertSame(map, obj.getMap());
    }

    @Test
    public void testCreateFromMapCharSequence() {
        Map<String, Object> map = new HashMap<>();
        map.put("foo", "bar");
        map.put("quux", 123);
        map.put("eeek", new StringBuilder("blah"));
        BsonObject obj = new BsonObject(map);
        assertEquals("bar", obj.getString("foo"));
        assertEquals(Integer.valueOf(123), obj.getInteger("quux"));
        assertEquals("blah", obj.getString("eeek"));
        assertSame(map, obj.getMap());
    }

    @Test
    public void testCreateFromMapNestedBsonObject() {
        Map<String, Object> map = new HashMap<>();
        BsonObject nestedObj = new BsonObject().put("foo", "bar");
        map.put("nested", nestedObj);
        BsonObject obj = new BsonObject(map);
        BsonObject nestedRetrieved = obj.getBsonObject("nested");
        assertEquals("bar", nestedRetrieved.getString("foo"));
    }

    @Test
    public void testCreateFromMapNestedMap() {
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("foo", "bar");
        map.put("nested", nestedMap);
        BsonObject obj = new BsonObject(map);
        BsonObject nestedRetrieved = obj.getBsonObject("nested");
        assertEquals("bar", nestedRetrieved.getString("foo"));
    }

    @Test
    public void testCreateFromMapNestedBsonArray() {
        Map<String, Object> map = new HashMap<>();
        BsonArray nestedArr = new BsonArray().add("foo");
        map.put("nested", nestedArr);
        BsonObject obj = new BsonObject(map);
        BsonArray nestedRetrieved = obj.getBsonArray("nested");
        assertEquals("foo", nestedRetrieved.getString(0));
    }

    @Test
    public void testCreateFromMapNestedList() {
        Map<String, Object> map = new HashMap<>();
        List<String> nestedArr = Arrays.asList("foo");
        map.put("nested", nestedArr);
        BsonObject obj = new BsonObject(map);
        BsonArray nestedRetrieved = obj.getBsonArray("nested");
        assertEquals("foo", nestedRetrieved.getString(0));
    }

    @Test
    public void testNumberEquality() {
        assertNumberEquals(4, 4);
        assertNumberEquals(4, (long) 4);
        assertNumberEquals(4, 4f);
        assertNumberEquals(4, 4D);
        assertNumberEquals((long) 4, (long) 4);
        assertNumberEquals((long) 4, 4f);
        assertNumberEquals((long) 4, 4D);
        assertNumberEquals(4f, 4f);
        assertNumberEquals(4f, 4D);
        assertNumberEquals(4D, 4D);
        assertNumberEquals(4.1D, 4.1D);
        assertNumberEquals(4.1f, 4.1f);
        assertNumberNotEquals(4.1f, 4.1D);
        assertNumberEquals(4.5D, 4.5D);
        assertNumberEquals(4.5f, 4.5f);
        assertNumberEquals(4.5f, 4.5D);
        assertNumberNotEquals(4, 5);
        assertNumberNotEquals(4, (long) 5);
        assertNumberNotEquals(4, 5D);
        assertNumberNotEquals(4, 5f);
        assertNumberNotEquals((long) 4, (long) 5);
        assertNumberNotEquals((long) 4, 5D);
        assertNumberNotEquals((long) 4, 5f);
        assertNumberNotEquals(4f, 5f);
        assertNumberNotEquals(4f, 5D);
        assertNumberNotEquals(4D, 5D);
    }

    private void assertNumberEquals(Number value1, Number value2) {
        BsonObject o1 = new BsonObject().put("key", value1);
        BsonObject o2 = new BsonObject().put("key", value2);
        if (!o1.equals(o2)) {
            fail("Was expecting " + value1.getClass().getSimpleName() + ":" + value1 + " == " +
                    value2.getClass().getSimpleName() + ":" + value2);
        }
        BsonArray a1 = new BsonArray().add(value1);
        BsonArray a2 = new BsonArray().add(value2);
        if (!a1.equals(a2)) {
            fail("Was expecting " + value1.getClass().getSimpleName() + ":" + value1 + " == " +
                    value2.getClass().getSimpleName() + ":" + value2);
        }
    }

    private void assertNumberNotEquals(Number value1, Number value2) {
        BsonObject o1 = new BsonObject().put("key", value1);
        BsonObject o2 = new BsonObject().put("key", value2);
        if (o1.equals(o2)) {
            fail("Was expecting " + value1.getClass().getSimpleName() + ":" + value1 + " != " +
                    value2.getClass().getSimpleName() + ":" + value2);
        }
    }

    @Test
    public void testBsonObjectEquality() {
        BsonObject obj = new BsonObject(Collections.singletonMap("abc", Collections.singletonMap("def", 3)));
        assertEquals(obj, new BsonObject(Collections.singletonMap("abc", Collections.singletonMap("def", 3))));
        assertEquals(obj, new BsonObject(Collections.singletonMap("abc", Collections.singletonMap("def", 3L))));
        assertEquals(obj, new BsonObject(Collections.singletonMap("abc", new BsonObject().put("def", 3))));
        assertEquals(obj, new BsonObject(Collections.singletonMap("abc", new BsonObject().put("def", 3L))));
        assertNotEquals(obj, new BsonObject(Collections.singletonMap("abc", Collections.singletonMap("def", 4))));
        assertNotEquals(obj, new BsonObject(Collections.singletonMap("abc", new BsonObject().put("def", 4))));
        BsonArray array = new BsonArray(Collections.singletonList(Collections.singletonMap("def", 3)));
        assertEquals(array, new BsonArray(Collections.singletonList(Collections.singletonMap("def", 3))));
        assertEquals(array, new BsonArray(Collections.singletonList(Collections.singletonMap("def", 3L))));
        assertEquals(array, new BsonArray(Collections.singletonList(new BsonObject().put("def", 3))));
        assertEquals(array, new BsonArray(Collections.singletonList(new BsonObject().put("def", 3L))));
        assertNotEquals(array, new BsonArray(Collections.singletonList(Collections.singletonMap("def", 4))));
        assertNotEquals(array, new BsonArray(Collections.singletonList(new BsonObject().put("def", 4))));
    }

    @Test
    public void testBsonObjectEquality2() {
        BsonObject obj1 = new BsonObject().put("arr", new BsonArray().add("x"));
        List<Object> list = new ArrayList<>();
        list.add("x");
        Map<String, Object> map = new HashMap<>();
        map.put("arr", list);
        BsonObject obj2 = new BsonObject(map);
        Iterator<Map.Entry<String, Object>> iter = obj2.iterator();
        // There was a bug where iteration of entries caused the underlying object to change resulting in a
        // subsequent equals changing
        while (iter.hasNext()) {
            Map.Entry<String, Object> entry = iter.next();
        }
        assertEquals(obj2, obj1);
    }

    @Test
    public void testPutInstantAsObject() {
        Object instant = Instant.now();
        BsonObject BsonObject = new BsonObject();
        BsonObject.put("instant", instant);
        // assert data is stored as String
        assertTrue(BsonObject.getValue("instant") instanceof String);
    }

    @Test
    public void testStreamCorrectTypes() throws Exception {
        BsonObject object = new BsonObject();
        object.put("object1", new BsonObject().put("object2", 12));

        testStreamCorrectTypes(object.copy());
        testStreamCorrectTypes(object);
    }

    @Test
    public void testRemoveMethodReturnedObject() {
        BsonObject obj = new BsonObject();
        obj.put("simple", "bar")
                .put("object", new BsonObject().put("name", "vert.x").put("count", 2))
                .put("array", new BsonArray().add(1.0).add(2.0));

        Object removed = obj.remove("missing");
        assertNull(removed);

        removed = obj.remove("simple");
        assertTrue(removed instanceof String);

        removed = obj.remove("object");
        assertTrue(removed instanceof BsonObject);
        assertEquals(((BsonObject) removed).getString("name"), "vert.x");

        removed = obj.remove("array");
        assertTrue(removed instanceof BsonArray);
        assertEquals(((BsonArray) removed).getDouble(0), 1.0, 0.0);
    }

    private void testStreamCorrectTypes(BsonObject object) {
        object.stream().forEach(entry -> {
            String key = entry.getKey();
            Object val = entry.getValue();
            assertEquals("object1", key);
            assertTrue("Expecting BsonObject, found: " + val.getClass().getCanonicalName(), val instanceof BsonObject);
        });
    }

    private BsonObject createBsonObject() {
        BsonObject obj = new BsonObject();
        obj.put("mystr", "bar");
        obj.put("myint", Integer.MAX_VALUE);
        obj.put("mylong", Long.MAX_VALUE);
        obj.put("myfloat", Float.MAX_VALUE);
        obj.put("mydouble", Double.MAX_VALUE);
        obj.put("myboolean", true);
        obj.put("mybinary", TestUtils.randomByteArray(100));
        obj.put("myinstant", Instant.now());
        return obj;
    }

}


