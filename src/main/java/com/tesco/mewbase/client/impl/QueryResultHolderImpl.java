package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.QueryResultHolder;

/**
 * Created by Jamie on 11/11/2016.
 */
public class QueryResultHolderImpl implements QueryResultHolder {
    private BsonObject document;

    public QueryResultHolderImpl(BsonObject document) {
        this.document = document;
    }

    @Override
    public BsonObject document() {
        return document;
    }
}
