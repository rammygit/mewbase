package com.tesco.mewbase.common;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 23/09/16.
 */
public interface Transactional {

    boolean startTx();

    CompletableFuture<Boolean> commitTx();

    CompletableFuture<Boolean> abortTx();
}
