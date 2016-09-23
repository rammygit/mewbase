package com.tesco.mubase.client;

import com.tesco.mubase.common.BsonObject;
import com.tesco.mubase.common.Transactional;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 22/09/16.
 */
public interface Producer extends Transactional {

    CompletableFuture<Boolean> emit(String eventType, BsonObject event);

}
