package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.client.QueryResult;
import com.tesco.mewbase.client.QueryResultHolder;

import java.util.function.Consumer;

/**
 * Created by Jamie on 11/11/2016.
 */
public class QueryResultImpl implements QueryResult {
    private int expectedNumResults;
    private int receivedNumResults = 0;
    private Consumer<QueryResultHolder> resultHandler;

    public QueryResultImpl(Consumer<QueryResultHolder> resultHandler, int expectedNumResults) {
        this.expectedNumResults = expectedNumResults;
        this.resultHandler = resultHandler;
    }

    @Override
    public void handle(QueryResultHolder resultHolder) {
        receivedNumResults++;
        resultHandler.accept(resultHolder);
    }

    @Override
    public boolean done() {
        return receivedNumResults >= expectedNumResults;
    }

    @Override
    public int numResults() {
        return expectedNumResults;
    }
}
