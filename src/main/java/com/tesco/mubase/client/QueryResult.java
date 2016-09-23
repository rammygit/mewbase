package com.tesco.mubase.client;

import java.util.function.Consumer;

/**
 * Created by tim on 22/09/16.
 */
public interface QueryResult {

    int numResults();

    void resultHandler(Consumer<QueryResultHolder> resultHandler);
}
