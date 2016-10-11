package com.tesco.mewbase.log;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.common.ReadStream;
import com.tesco.mewbase.common.WriteStream;

import java.util.concurrent.CompletableFuture;

/**
 * Created by tim on 27/09/16.
 */
public interface Log {

    ReadStream openReadStream(long pos);

    WriteStream openWriteStream();

    CompletableFuture<Void> append(BsonObject obj);
}
