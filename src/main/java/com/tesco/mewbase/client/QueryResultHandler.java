package com.tesco.mewbase.client;

/**
 * Created by tim on 22/09/16.
 */
public interface QueryResultHandler {

    int numResults();

    boolean isDone();

    void handle(QueryResult resultHolder);
}
