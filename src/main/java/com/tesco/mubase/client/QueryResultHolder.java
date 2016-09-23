package com.tesco.mubase.client;

import com.tesco.mubase.common.BsonObject;

/**
 * Created by tim on 22/09/16.
 */
public interface QueryResultHolder {

    BsonObject document();

    void done();
}
