package com.tesco.mewbase.doc.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A binder is a collection of named documents
 * <p>
 * Created by tim on 30/09/16.
 */
public class Binder {

    private final Map<String, BsonObject> docs = new ConcurrentHashMap<>();

    public BsonObject getDocument(String id) {
        return docs.get(id);
    }

    /**
     * Save a document
     *
     * @param id  the identify for the document
     * @param doc the document
     */
    public void save(String id, BsonObject doc) {
        docs.put(id, doc);
    }

    public boolean delete(String id) {
        return docs.remove(id) != null;
    }
}
