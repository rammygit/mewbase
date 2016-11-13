package com.tesco.mewbase.client.impl;

import com.tesco.mewbase.client.QueryResultHandler;
import com.tesco.mewbase.client.QueryResult;

import java.util.function.Consumer;

/**
 * Created by Jamie on 11/11/2016.
 */
public class QueryResultHandlerImpl implements QueryResultHandler {
    private int expectedNumResults;
    private int receivedNumResults = 0;
    private Consumer<QueryResult> resultHandler;

    public QueryResultHandlerImpl(Consumer<QueryResult> resultHandler, int expectedNumResults) {
        this.expectedNumResults = expectedNumResults;
        this.resultHandler = resultHandler;
    }

    @Override
    public void handle(QueryResult resultHolder) {
        receivedNumResults++;
        resultHandler.accept(resultHolder);
    }

    @Override
    public boolean isDone() {
        return receivedNumResults >= expectedNumResults;
    }

    @Override
    public int numResults() {
        return expectedNumResults;
    }
}
