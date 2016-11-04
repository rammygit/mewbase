package com.tesco.mewbase.doc.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by tim on 30/09/16.
 */
public class Binder {

    private final Map<String, BsonObject> docs = new ConcurrentHashMap<>();

    public BsonObject getDocument(String id) {
        return docs.get(id);
    }

    public void save(String id, BsonObject doc) {
        docs.put(id, doc);
    }
}
