package com.tesco.mewbase.doc;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.DocQuerier;

import java.util.concurrent.CompletableFuture;

/**
 *
 * Created by tim on 30/09/16.
 */
public interface DocManager extends DocQuerier {

    //TODO: should probable be public static final
    String ID_FIELD = "id";

  /**
   * Save a named document to a binder with the given name
   * @param binder name of binder to save the document to
   * @param id the name of the document within the binder
   * @param doc the document to save
   * @return
   */
    CompletableFuture<Void> save(String binder, String id, BsonObject doc);
}
