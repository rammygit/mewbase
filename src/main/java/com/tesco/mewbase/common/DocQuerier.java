package com.tesco.mewbase.common;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.client.QueryResult;

import java.util.concurrent.CompletableFuture;

/**
 * Used for finding documents within a given binder
 *
 * Created by tim on 23/09/16.
 */
public interface DocQuerier {

    // TODO use RxJava for this too

  /**
   * Retrieve a document by its binder name and its identifier within that binder
   *
   * @param binderName the name of the binder
   * @param id the name of the document within the binder
   * @return
   */
    CompletableFuture<BsonObject> findByID(String binderName, String id);

  /**
   * Find matching documents within a given binder
   * TODO: work out the structure of a matcher
   * @param binderName the name of the binder to run the matcher against
   * @param matcher
   * @return
   */

  CompletableFuture<QueryResult> findMatching(String binderName, BsonObject matcher);

}
