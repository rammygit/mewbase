package com.tesco.mewbase.client;

import java.util.function.Consumer;

/**
 * Created by tim on 22/09/16.
 */
public interface QueryResult {

    int numResults();

    boolean done();

    void handle(QueryResultHolder resultHolder);
}
