package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.QueryResult;

/**
 * Created by Jamie on 11/11/2016.
 */
public class QueryResultImpl implements QueryResult {
    private BsonObject document;
    private Runnable acknowledger;

    public QueryResultImpl(BsonObject document) {
        this.document = document;
    }

    @Override
    public BsonObject document() {
        return document;
    }

    @Override
    public void acknowledge() {
        acknowledger.run();
    }

    public void onAcknowledge(Runnable acknowledger) {
        this.acknowledger = acknowledger;
    }
}
